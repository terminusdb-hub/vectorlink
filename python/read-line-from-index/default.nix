{ pkgs, ... }:
with pkgs;
with pkgs.python311Packages;
buildPythonPackage rec {
  name = "create_vectorlink_task";
  src = ./.;
  format = "pyproject";
  propagatedBuildInputs = [
    poetry-core
    boto3
  ];
}
