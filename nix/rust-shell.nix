{pkgs}:
with pkgs;
mkShell {
  buildInputs = [
    rust-bin.nightly.latest.minimal
  ];
}
