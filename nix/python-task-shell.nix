{pkgs}:
with pkgs;
with pkgs.python311Packages;
mkShell {
  buildInputs = [
    python311
    vectorlink.vectorlink-task-py
  ];
}