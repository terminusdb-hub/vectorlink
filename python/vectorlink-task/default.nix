{ pkgs, ... }:
with pkgs;
with pkgs.python311Packages;
buildPythonPackage rec {
  name = "vectorlink_task";
  src = ./.;
  format = "pyproject";
  propagatedBuildInputs = [
    poetry-core
    etcd3
    protobuf
  ];
}
