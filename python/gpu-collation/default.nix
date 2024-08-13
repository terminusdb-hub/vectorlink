{ pkgs }:
with pkgs;
with pkgs.python311Packages;
buildPythonPackage rec {
  name = "vectorlink-collation";
  src = ./.;
  format = "pyproject";
  propagatedBuildInputs = [
    poetry-core
    numpy
    torch
    transformers
    accelerate
    sentence-transformers
    boto3
    pybars3
    vectorlink.vectorlink-task-py
  ];
}
