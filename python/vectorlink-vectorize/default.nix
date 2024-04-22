{ pkgs }:
pkgs.mkPoetryApplication {
  projectDir = ./.;
  preferWheels = true;
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
