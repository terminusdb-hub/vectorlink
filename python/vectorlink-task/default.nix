{ pkgs }:
with pkgs;
with pkgs.python311Packages;
buildPythonPackage rec {
  name = "vectorlink_vectorize";
  src = ./.;
  format = "pyproject";
  propagatedBuildInputs = [
    poetry-core
    etcd3
    protobuf
  ];
}
