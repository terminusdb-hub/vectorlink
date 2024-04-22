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

  outputs = { self, nixpkgs, crane, rust-overlay, poetry2nix }: (
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      rustFlagsFor = {
        x86_64-linux = "-C target-feature=+sse3,+avx,+avx2";
        aarch64-linux = "-C target-feature=+v7,+neon";
      };
      nixpkgsFor = forAllSystems (system:
        import nixpkgs {
          inherit system;
          overlays = [
            (import rust-overlay)
            (final: prev: rec {
              craneLib = (crane.mkLib prev).overrideToolchain final.rust-bin.nightly.latest.minimal;
              rust-args = {
                nativeBuildInputs = [
                  final.pkg-config
                  final.protobuf
                ];
                buildInputs = [
                  final.openssl
                ];
                RUSTFLAGS = rustFlagsFor.${system};
                src = craneLib.cleanCargoSource (craneLib.path ./.);
                strictDeps = true;
                doCheck = false;
              };
              vl-workspace = craneLib.buildDepsOnly (rust-args // {
                pname = "vectorlink";
                version = "0.1.0";
              });
              inherit (poetry2nix.lib.mkPoetry2Nix { pkgs = final; }) mkPoetryApplication defaultPoetryOverrides;
            })
          ];
        });
    in
    {
      packages = forAllSystems (system :
        let pkgs = nixpkgsFor.${system};
        in
        {
          vectorlink = pkgs.callPackage vectorlink/default.nix {};
          vectorlink-worker = pkgs.callPackage vectorlink-worker/default.nix {};
          vectorlink-infra = pkgs.callPackage vectorlink-infra/default.nix {};
          vectorlink-task-monitor = pkgs.callPackage python/vectorlink-task/default.nix {};
          vectorlink-vectorize = pkgs.callPackage python/vectorlink-vectorize/default.nix {};
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
