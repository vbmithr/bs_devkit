(*---------------------------------------------------------------------------
   Copyright (c) 2016 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
   %%NAME%% %%VERSION%%
  ---------------------------------------------------------------------------*)
open Core
open Async

type side = [`Buy | `Sell] [@@deriving sexp, bin_io]
let pp_side ppf side =
  Format.fprintf ppf "%s" (match side with `Buy -> "Buy" | `Sell -> "Sell")

let add_suffix fn ~suffix =
  let open Filename in
  match split_extension fn with main_part, extension ->
    main_part ^ suffix ^
    Option.value_map extension ~default:"" ~f:(fun e -> "." ^ e)

let loglevel_of_int = function 2 -> `Info | 3 -> `Debug | _ -> `Error

(* Only works when the string represents an positive integer or a
   positive float with all decimals (including zeros) corresponding to
   [mult] *)
let satoshis_of_string ?(mult=100_000_000) fstr =
  let fstr_buf = Bytes.of_string fstr in
  match String.index fstr '.' with
  | None -> Int.of_string fstr * mult
  | Some idx ->
    Bytes.blito ~src:fstr_buf ~dst:fstr_buf ~src_pos:0 ~dst_pos:1 ~src_len:idx ();
    Bytes.set fstr_buf 0 '0';
    Int.of_string (Bytes.unsafe_to_string ~no_mutation_while_string_reachable:fstr_buf)

let robust_int_of_float_exn precision mult f =
  let s = Printf.sprintf "%+.*f" precision f |>
          Bytes.unsafe_of_string_promise_no_mutation in
  let sign = Bytes.get s 0 in
  Bytes.set s 0 '0';
  match sign with
  | '+' -> satoshis_of_string ~mult (Bytes.unsafe_to_string ~no_mutation_while_string_reachable:s)
  | '-' -> Int.neg @@ satoshis_of_string ~mult (Bytes.unsafe_to_string ~no_mutation_while_string_reachable:s)
  | _ -> invalid_arg "robust_int_of_float_exn: sign"

let satoshis_int_of_float_exn = robust_int_of_float_exn 8 100_000_000
let bps_int_of_float_exn = robust_int_of_float_exn 4 10_000

let seconds_int63_of_ts ts =
  let open Int63 in
  Time_ns.to_int63_ns_since_epoch ts / of_int 1_000_000_000

let seconds_int32_of_ts ts =
  Int63.to_int32_exn (seconds_int63_of_ts ts)

let seconds_float_of_ts ts =
  Int63.to_float (seconds_int63_of_ts ts)

let seconds_int64_of_ts ts =
  Int63.to_int64 (seconds_int63_of_ts ts)

let eat_exn ?log ?on_exn f =
  Monitor.try_with_or_error f >>= function
  | Ok () -> Deferred.unit
  | Error err ->
    Option.iter log ~f:(fun log -> Log.error log "%s" Error.(to_string_hum err));
    match on_exn with
    | None -> Deferred.unit
    | Some f -> Error.to_exn err |> Monitor.extract_exn |> f

let vwap side ?(vlimit=Int.max_value) =
  let fold_f ~key:p ~data:v (vwap, vol) =
    if vol >= vlimit then (vwap, vol)
    else
      let v = Int.min v (vlimit - vol) in
      vwap + p * v, vol + v
  in
  let fold = Int.Map.(match side with `Buy -> fold_right | `Sell -> fold) in
  fold ~init:(0, 0) ~f:fold_f

module Cfg = struct
  type cfg = {
    key: string ;
    secret: string ;
    passphrase: string [@default ""];
    quote: (string * int) list [@default []];
  } [@@deriving sexp]

  type t = (string * cfg) list [@@deriving sexp]
end

module OB = struct
  type action = Partial | Insert | Update | Delete [@@deriving sexp, bin_io]

  type update = {
    id: int;
    side: side;
    price: int [@default 0]; (* in satoshis *)
    size: int [@default 0] (* in contracts or in tick size *);
  } [@@deriving sexp, bin_io]

  type t = {
    action: action;
    data: update;
  } [@@deriving sexp, bin_io]
end

module DB = struct
  type book_entry = {
    side: side;
    price: int;
    qty: int;
  } [@@deriving sexp, bin_io]

  let pp_book_entry ppf { side ; price ; qty } =
    let price = price // 100_000_000 in
    let qty = qty // 100_000_000 in
    Format.fprintf ppf "%a %.8f %.8f" pp_side side price qty

  type trade = {
    ts: Time_ns.t;
    side: side;
    price: int; (* in satoshis *)
    qty: int; (* in satoshis *)
  } [@@deriving sexp, bin_io]

  let pp_trade ppf { ts ; side ; price ; qty } =
    let price = price // 100_000_000 in
    let qty = qty // 100_000_000 in
    Format.fprintf ppf "%a %.8f %.8f %a" pp_side side price qty Time_ns.pp ts

  type entry =
    | BModify of book_entry
    | BRemove of book_entry
    | Trade of trade [@@deriving sexp, bin_io]

  let pp ppf = function
  | BModify entry -> Format.fprintf ppf "BModify %a" pp_book_entry entry
  | BRemove entry -> Format.fprintf ppf "BDelete %a" pp_book_entry entry
  | Trade trade -> Format.fprintf ppf "Trade %a" pp_trade trade

  type entry_list = entry list [@@deriving sexp, bin_io]
  type db = entry list Int.Map.t [@@deriving sexp, bin_io]
end

let loop_log_errors ?log f =
  let rec inner () =
    Monitor.try_with_or_error ~name:"loop_log_errors" f >>= function
    | Ok _ -> Deferred.unit
    | Error err ->
        Option.iter log ~f:(fun log -> Log.error log "run: %s" @@ Error.to_string_hum err);
        inner ()
  in inner ()

let conduit_server ~tls ~crt_path ~key_path =
  if tls then
    Sys.file_exists crt_path >>= fun crt_exists ->
    Sys.file_exists key_path >>| fun key_exists ->
    match crt_exists, key_exists with
    | `Yes, `Yes -> `OpenSSL (`Crt_file_path crt_path, `Key_file_path key_path)
    | _ -> failwith "TLS crt/key file not found"
  else
  return `TCP

let price_display_format_of_ticksize tickSize =
  (**) if tickSize =. (1//2) then `price_display_format_denominator_2
  else if tickSize =. (1//4) then `price_display_format_denominator_4
  else if tickSize =. (1//8) then `price_display_format_denominator_8
  else if tickSize =. (1//16) then `price_display_format_denominator_16
  else if tickSize =. (1//32) then `price_display_format_denominator_32
  else if tickSize =. (1//64) then `price_display_format_denominator_64
  else if tickSize =. (1//128) then `price_display_format_denominator_128
  else if tickSize =. (1//256) then `price_display_format_denominator_256
  else if tickSize >=. 1. then `price_display_format_decimal_0
  else if tickSize >=. 1e-1 then `price_display_format_decimal_1
  else if tickSize >=. 1e-2 then `price_display_format_decimal_2
  else if tickSize >=. 1e-3 then `price_display_format_decimal_3
  else if tickSize >=. 1e-4 then `price_display_format_decimal_4
  else if tickSize >=. 1e-5 then `price_display_format_decimal_5
  else if tickSize >=. 1e-6 then `price_display_format_decimal_6
  else if tickSize >=. 1e-7 then `price_display_format_decimal_7
  else if tickSize >=. 1e-8 then `price_display_format_decimal_8
  else `price_display_format_decimal_9

module Arg_type = struct
  let uri = Command.Arg_type.create Uri.of_string
  let span = Command.Arg_type.create Time_ns.Span.of_string
  let loglevel = Command.Arg_type.create begin fun s ->
      match Logs.level_of_string s with
      | Ok (Some level) -> level
      | Ok None -> Logs.App
      | Error (`Msg msg) -> failwithf "Unknown level %s" msg ()
    end
end

let sexp_of_loglevel ll =
  sexp_of_string (Logs.level_to_string (Some ll))

(*---------------------------------------------------------------------------
   Copyright (c) 2016 Vincent Bernardoff

   Permission to use, copy, modify, and/or distribute this software for any
   purpose with or without fee is hereby granted, provided that the above
   copyright notice and this permission notice appear in all copies.

   THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
   WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
   MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
   ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
   WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
   ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
   OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
  ---------------------------------------------------------------------------*)
