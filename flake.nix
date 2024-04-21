{
  description = "Vectorlink projects and machines";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-23.11";
    naersk.url = "github:nix-community/naersk";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, rust-overlay, naersk }: (
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      nixpkgsFor = forAllSystems (system:
        import nixpkgs {
          inherit system;
          overlays = [
            rust-overlay.overlays.default
            (final: prev: { naersk = prev.callPackage naersk {
                              rustc = prev.rust-bin.nightly.latest.default;
                              cargo = prev.rust-bin.nightly.latest.default;
                            }; })
          ];
        });
    in
    {
      packages = forAllSystems (system :
        let pkgs = nixpkgsFor.${system};
        in
        {
          hello = pkgs.hello;
          vectorlink-worker = pkgs.callPackage vectorlink-worker/default.nix {};
        }
      );
    }
  );
}
