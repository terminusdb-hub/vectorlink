{ pkgs }:
with pkgs.python311Packages;
buildPythonPackage rec {
  name = "vectorlink-vectorize";
  pyproject = true;
  src = ./.;

  # honestly unsure why this can't be dependencies instead, but this
  # works.
  propagatedBuildInputs = [
    numpy
    torch
    transformers
    accelerate
    sentence-transformers
  ];

  nativeBuildInputs = [
    poetry-core
  ];
}
