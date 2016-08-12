open Core.Std
open Async.Std
open Dtc

module Writer = struct
  include Writer
  let write_cstruct w cs =
    let open Cstruct in
    write_bigstring ~pos:cs.off ~len:cs.len w cs.buffer
end

module I64S = struct
  module T = struct
    type t = Int64.t * String.t [@@deriving sexp]
    let compare = compare
    let hash = Hashtbl.hash
  end
  include T
  include Hashable.Make (T)
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
  open Core.Std
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
    | #json -> invalid_arg "float_exn"

  let float_or_null_exn ~default t field =
    match String.Map.find_exn t field with
    | `Float f -> f
    | `Int i -> Float.of_int i
    | `Intlit il -> Float.of_string il
    | `Null -> default
    | #json -> invalid_arg "float_exn"

  let bool t field =
    Option.bind (String.Map.find t field)
      (function `Bool b -> Some b | #json -> None)

  let bool_exn t field =
    match String.Map.find_exn t field with
    | `Bool b -> b
    | #json -> invalid_arg "bool_exn"

  let string t field =
    Option.bind (String.Map.find t field)
      (function `String s -> Some s | #json -> None)

  let string_exn t field =
    match String.Map.find_exn t field with
    | `String s -> s
    | #json -> invalid_arg "string_exn"

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
    | #Yojson.Safe.json -> invalid_arg "int64_exn"

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

let robust_int_of_float_exn (type t) (module I : Int_intf.S with type t = t) precision f =
  let s = Printf.sprintf "%.*f" precision f in
  let i = String.index_exn s '.' in
  let op = if String.get s 0 = '-' then I.(-) else I.(+) in
  let a = I.of_string @@ String.sub s 0 i in
  let b = I.of_string @@ String.sub s (i+1) (String.length s - i - 1) in
  I.(op (a * (pow (of_int_exn 10) (of_int_exn precision))) b)

let satoshis_of_float_exn = robust_int_of_float_exn (module Int64) 8
let satoshis_int_of_float_exn = robust_int_of_float_exn (module Int) 8
let bps_int_of_float_exn = robust_int_of_float_exn (module Int) 4

let float_of_ts ts = Time_ns.to_int_ns_since_epoch ts |> Float.of_int |> fun date -> date /. 1e9
let int_of_ts ts = Time_ns.to_int_ns_since_epoch ts / 1_000_000_000
let int32_of_ts ts = int_of_ts ts |> Int32.of_int_exn

let maybe_debug log k = Printf.ksprintf (fun msg -> Option.iter log ~f:(fun log -> Log.debug log "%s" msg)) k
let maybe_info log k = Printf.ksprintf (fun msg -> Option.iter log ~f:(fun log -> Log.info log "%s" msg)) k
let maybe_error log k = Printf.ksprintf (fun msg -> Option.iter log ~f:(fun log -> Log.error log "%s" msg)) k

let eat_exn ?log ?on_exn f =
  Monitor.try_with_or_error f >>= function
  | Ok () -> Deferred.unit
  | Error err ->
    maybe_error log "%s" Error.(to_string_hum err);
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
      let nb_of_evts = List.length evts in
      let nb_written = Bin_prot.Utils.bin_write_size_header scratch ~pos:0 nb_of_evts in
      let scratch_shared = Bigstring.sub_shared scratch ~len:nb_written in
      Bigbuffer.add_bigstring buf scratch_shared;
      List.iter evts ~f:begin fun e ->
        let nb_written = bin_write_t scratch ~pos:0 e in
        let scratch_shared = Bigstring.sub_shared scratch ~len:nb_written in
        Bigbuffer.add_bigstring buf scratch_shared;
      end;
      LevelDB.put ?sync db key @@ Bigbuffer.contents buf
end
