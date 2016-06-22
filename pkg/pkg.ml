#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe "bs_devkit" @@ fun c ->
  Ok [
    Pkg.lib ~exts:Exts.library "src/bs_devkit_core";
    Pkg.mllib "src/bs_devkit.mllib"
  ]
