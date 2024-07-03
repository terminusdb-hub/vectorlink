{ pkgs ? (import <nixpkgs> {
  overlays = [ import ./nix/overlay.nix ];
}) }:
with pkgs;
mkShell {
  nativeBuildInputs = [
    pkg-config
    protobuf
    (rust-bin.stable.latest.default.override {
      extensions = [ "rust-src" "rust-analyzer" ];
    })

  ];
  buildInputs = [
    openssl
    gmp
  ];
}
