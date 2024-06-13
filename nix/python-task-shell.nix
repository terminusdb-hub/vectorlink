{pkgs}:
with pkgs;
with pkgs.python311Packages;
mkShell {
  buildInputs = [
    python311
    (pkgs.callPackage ../vectorlink-task-py {})
  ];
}
