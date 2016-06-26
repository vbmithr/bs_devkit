open Core.Std
open Async.Std

open Cohttp_async

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

let b64_of_uuid uuid_str =
  match Uuidm.of_string uuid_str with
  | None -> invalid_arg "Uuidm.of_string"
  | Some uuid -> Uuidm.to_bytes uuid |> B64.encode

let uuid_of_b64 b64 =
  B64.decode b64 |> Uuidm.of_bytes |> function
  | None -> invalid_arg "uuid_of_b64"
  | Some uuid -> Uuidm.to_string uuid

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
  try_with f >>= function
  | Ok () -> Deferred.unit
  | Error exn ->
    maybe_error log "%s" Exn.(backtrace ());
    match on_exn with
    | None -> Deferred.unit
    | Some f -> f exn

module rec Side : sig
  type t = Bid | Ask [@@deriving show, bin_io]
  val other : t -> t
  val of_buyorsell : BuyOrSell.t -> t
end = struct
  type t =
    | Bid [@printer fun fmt _ -> Format.pp_print_string fmt "Bid"]
    | Ask [@printer fun fmt _ -> Format.pp_print_string fmt "Ask"]
    [@@deriving show, bin_io]

  let other = function Bid -> Ask | Ask -> Bid
  let of_buyorsell = function BuyOrSell.Buy -> Bid | Sell -> Ask
end
and BuyOrSell : sig
  type t = Buy | Sell [@@deriving show, bin_io]
  val other : t -> t
  val of_side : Side.t -> t
end = struct
  type t =
    | Buy [@printer fun fmt _ -> Format.pp_print_string fmt "Buy"]
    | Sell [@printer fun fmt _ -> Format.pp_print_string fmt "Sell"]
    [@@deriving show, bin_io]

  let other = function Buy -> Sell | Sell -> Buy
  let of_side = function Side.Bid -> Buy | Ask -> Sell
end

let vwap side ?(vlimit=Int.max_value) =
  let fold_f ~key:p ~data:v (vwap, vol) =
    if vol >= vlimit then (vwap, vol)
    else
      let v = min v (vlimit - vol) in
      vwap + p * v, vol + v
  in
  let fold = Int.Map.(match side with Side.Bid -> fold_right | Ask -> fold) in
  fold ~init:(0, 0) ~f:fold_f

let presult_exn = function
  | `Ok a -> a
  | `Error msg -> invalid_arg msg
