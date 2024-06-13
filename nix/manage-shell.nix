{pkgs, vectorlink}:
with pkgs;
with pkgs.python311Packages;
mkShell {
  buildInputs = [
    python311
    vectorlink.vectorlink-task-py
    vectorlink.task-util
    vectorlink.create-vectorize-tasks
    vectorlink.read-line-from-index
  ];
}
