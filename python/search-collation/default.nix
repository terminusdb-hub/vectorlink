{ pkgs }:
with pkgs;
with pkgs.python311Packages;
buildPythonPackage rec {
  name = "search-collation";
  src = ./.;
  format = "pyproject";
  propagatedBuildInputs = [
    poetry-core
    numpy
    torch
    accelerate
    sentence-transformers
    vectorlink.vectorlink-task-py
  ];
}
