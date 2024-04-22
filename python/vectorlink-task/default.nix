{ pkgs }:
#with pkgs.python311Packages;
#buildPythonPackage rec {
#  name = "vectorlink-task-monitor";
#  src = ./.;
#  format = "pyproject";
#}
pkgs.mkPoetryApplication {
  projectDir = ./.;
  overrides = pkgs.defaultPoetryOverrides.extend
    (final: prev: {
      etcd3 = prev.etcd3.overridePythonAttrs
      (
        old: {
          buildInputs = (old.buildInputs or [ ]) ++ [ final.setuptools ];
        }
      );
    });
}
