{ craneLib, rust-args }:
craneLib.buildDepsOnly (rust-args // {
  pname = "vectorlink";
  version = "0.1.0";
})
