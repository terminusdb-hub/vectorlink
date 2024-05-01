{
  description = "Vectorlink projects and machines";

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
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, crane, rust-overlay, poetry2nix }@inputs: (
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
        {
          vectorlink = pkgs.callPackage ./vectorlink {};
          vectorlink-worker = pkgs.callPackage ./vectorlink-worker {};
          vectorlink-infra = pkgs.callPackage ./vectorlink-infra {};
          vectorlink-task-monitor = pkgs.callPackage python/vectorlink-task {};
          vectorlink-vectorize = pkgs.callPackage python/vectorlink-vectorize {};
        }
      );

      apps = forAllSystems (system :
        let p = self.packages.${system}; in
        {
          worker = {
            type = "app";
            program = "${p.vectorlink-worker}/bin/vectorlink-worker";
          };
          task-monitor = {
            type = "app";
            program = "${p.vectorlink-task-monitor}/bin/task-monitor";
          };
          backend = {
            type = "app";
            program = "${p.vectorlink-vectorize}/bin/backend";
          };
        });
    }
  );
}
