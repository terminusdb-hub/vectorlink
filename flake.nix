{
  description = "Vectorlink projects";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-23.11";
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs = { self, nixpkgs, crane, rust-overlay }@inputs: (
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      makeOverlay = (import nix/overlay.nix) ./. inputs;
      nixpkgsFor = forAllSystems makeOverlay;
    in
    {
      overlays = nixpkgsFor;
      packages = forAllSystems (system:
        let pkgs = nixpkgsFor.${system};
        in
        rec {
          vectorlink = pkgs.callPackage ./vectorlink {};
          default = vectorlink;
        }
      );
    }
  );
}
