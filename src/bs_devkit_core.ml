open Core
open Async
open Dtc

module Writer = struct
  include Writer
  let write_cstruct w cs =
    let open Cstruct in
    write_bigstring ~pos:cs.off ~len:cs.len w cs.buffer
end

module InetAddr = struct
  module T = struct
    include Socket.Address.Inet
    let hash = Hashtbl.hash
  end
  include T
  include Hashable.Make (T)
end

module RespObj = struct
  open Core
  open String.Map

  type json = Yojson.Safe.json
  type t = json String.Map.t

  let float t field =
    Option.bind (String.Map.find t field)
      (function
        | `Float f -> Some f
        | `Int i -> Some Float.(of_int i)
        | `Intlit il -> Some Float.(of_string il)
        | #json -> None
      )

  let float_exn t field =
    match String.Map.find_exn t field with
    | `Float f -> f
    | `Int i -> Float.of_int i
    | `Intlit il -> Float.of_string il
    | json -> invalid_argf "float_exn: got %s" (Yojson.Safe.to_string json) ()

  let float_or_null_exn ~default t field =
    match String.Map.find_exn t field with
    | `Float f -> f
    | `Int i -> Float.of_int i
    | `Intlit il -> Float.of_string il
    | `Null -> default
    | json -> invalid_argf "float_exn: got %s" (Yojson.Safe.to_string json) ()

  let bool t field =
    Option.bind (String.Map.find t field)
      (function `Bool b -> Some b | #json -> None)

  let bool_exn t field =
    match String.Map.find_exn t field with
    | `Bool b -> b
    | json -> invalid_argf "bool_exn: got %s" (Yojson.Safe.to_string json) ()

  let string t field =
    Option.bind (String.Map.find t field)
      (function `String s -> Some s | #json -> None)

  let string_exn t field =
    match String.Map.find_exn t field with
    | `String s -> s
    | json -> invalid_argf "string_exn: got %s" (Yojson.Safe.to_string json) ()

  let int t field =
    Option.bind (String.Map.find t field)
      (function
        | `Int i -> Some i
        | `Intlit s -> Option.try_with (fun () -> Int.of_string s)
        | #json -> None
      )

  let int_exn t field =
    match String.Map.find_exn t field with
    | `Int i -> i
    | `Intlit s -> Int.of_string s
    | json -> invalid_argf "int_exn: got %s" (Yojson.Safe.to_string json) ()

  let int64 t field =
    Option.bind (String.Map.find t field)
      (function
        | `Int i -> Some Int64.(of_int i)
        | `Intlit s -> Some Int64.(of_string s)
        | #json -> None
      )

  let int64_exn t field =
    match String.Map.find_exn t field with
    | `Int i -> Int64.of_int i
    | `Intlit s -> Int64.of_string s
    | json -> invalid_argf "int64_exn: got %s" (Yojson.Safe.to_string json) ()

  let of_json = function
    | `Assoc fields ->
        (List.fold_left fields ~init:String.Map.empty
           ~f:(fun a (key, data) ->
               String.Map.add a ~key ~data)
        )
    | #json -> invalid_arg "of_json"

  let to_json o = `Assoc (String.Map.to_alist o)

  let merge t t' = String.Map.merge t t'
      ~f:(fun ~key:_ -> function
          | `Both (v, v') -> Some v'
          | `Left v -> Some v
          | `Right v -> Some v
        )
end

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
  match String.index fstr '.' with
  | None -> Int.of_string fstr * mult
  | Some idx ->
    String.blito ~src:fstr ~dst:fstr ~src_pos:0 ~dst_pos:1 ~src_len:idx ();
    String.set fstr 0 '0';
    Int.of_string fstr

let robust_int_of_float_exn precision mult f =
  let s = Printf.sprintf "%+.*f" precision f in
  let sign = String.get s 0 in
  String.set s 0 '0';
  match sign with
  | '+' -> satoshis_of_string ~mult s
  | '-' -> Int.neg @@ satoshis_of_string ~mult s
  | _ -> invalid_arg "robust_int_of_float_exn: sign"

let satoshis_int_of_float_exn = robust_int_of_float_exn 8 100_000_000
let bps_int_of_float_exn = robust_int_of_float_exn 4 10_000

let float_of_ts ts = Time_ns.to_int_ns_since_epoch ts |> Float.of_int |> fun date -> date /. 1e9
let int_of_ts ts = Time_ns.to_int_ns_since_epoch ts / 1_000_000_000
let int32_of_ts ts = int_of_ts ts |> Int32.of_int_exn

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
  let fold = Int.Map.(match side with Dtc.Buy -> fold_right | Sell -> fold) in
  fold ~init:(0, 0) ~f:fold_f

module Cfg = struct
  type cfg = {
    key: string;
    secret: string;
    passphrase: string [@default ""];
    quote: (string * int) list [@default []];
  } [@@deriving yojson]

  type t = (string * cfg) list [@@deriving yojson]
end

module OB = struct
  type action = Partial | Insert | Update | Delete [@@deriving sexp, bin_io]

  type update = {
    id: int;
    side: Dtc.side;
    price: int [@default 0]; (* in satoshis *)
    size: int [@default 0] (* in contracts or in tick size *);
  } [@@deriving create, sexp, bin_io]

  type t = {
    action: action;
    data: update;
  } [@@deriving create, sexp, bin_io]
end

module DB = struct
  type book_entry = {
    side: Dtc.side;
    price: int;
    qty: int;
  } [@@deriving create, sexp, bin_io]

  type trade = {
    ts: Time_ns.t;
    side: Dtc.side;
    price: int; (* in satoshis *)
    qty: int; (* in satoshis *)
  } [@@deriving create, sexp, bin_io]

  type t =
    | BModify of book_entry
    | BRemove of book_entry
    | Trade of trade [@@deriving sexp, bin_io]

  type t_list = t list [@@deriving sexp, bin_io]

  let make_store () =
    let key = String.create 8 in
    let buf = Bigbuffer.create 128 in
    let scratch = Bigstring.create 128 in
    fun ?sync db seq evts ->
      Bigbuffer.clear buf;
      Binary_packing.pack_signed_64_int_big_endian ~buf:key ~pos:0 seq;
      let nb_of_evts = List.length evts |> Bin_prot.Nat0.of_int in
      let nb_written = Bin_prot.Write.bin_write_nat0 scratch ~pos:0 nb_of_evts in
      let scratch_shared = Bigstring.sub_shared scratch ~len:nb_written in
      Bigbuffer.add_bigstring buf scratch_shared;
      List.iter evts ~f:begin fun e ->
        let nb_written = bin_write_t scratch ~pos:0 e in
        let scratch_shared = Bigstring.sub_shared scratch ~len:nb_written in
        Bigbuffer.add_bigstring buf scratch_shared;
      end;
      LevelDB.put ?sync db key @@ Bigbuffer.contents buf
end
