{
  description = "Vectorlink projects and machines";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-24.05";
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
      overlay = import nix/overlay.nix;
      nixpkgsFor = forAllSystems (system:
        (import nixpkgs) {
          inherit system;
          config = { allowUnfree = true; cudaEnabled = true; cudaSupport = true;};
          overlays = [
            (import rust-overlay)
            (final: prev: {
              craneLib = (crane.mkLib prev).overrideToolchain final.rust-bin.nightly.latest.minimal;
            })
            overlay
          ];
        }
      );
    in
    {
      overlays.default = overlay;

      packages = forAllSystems (system:
        nixpkgsFor.${system}.vectorlink
      );

      apps = forAllSystems (system :
        let p = self.packages.${system}; in
        {
          worker = {
            type = "app";
            program = "${p.vectorlink-worker}/bin/vectorlink-worker";
          };
          line-index = {
            type = "app";
            program = "${p.line-index}/bin/line-index";
          };
          task-monitor = {
            type = "app";
            program = "${p.vectorlink-task-monitor}/bin/task-monitor";
          };
          collation = {
            type = "app";
            program = "${p.search-collation}/bin/search-collation";
          };
          collation-server = {
            type = "app";
            program = "${p.search-collation}/bin/collation-server";
          };
          vectorize-server = {
            type = "app";
            program = "${p.vectorlink-vectorize}/bin/vectorize-server";
          };
          backend = {
            type = "app";
            program = "${p.vectorlink-vectorize}/bin/backend";
          };
          create-vectorize-tasks = {
            type = "app";
            program = "${p.create-vectorize-tasks}/bin/create-vectorize-tasks";
          };
          read-line-from-index = {
            type = "app";
            program = "${p.read-line-from-index}/bin/read-line-from-index";
          };
          print-search-results = {
            type = "app";
            program = "${p.print-search-results}/bin/print-search-results";
          };
        });

      devShells = forAllSystems (system :
        let pkgs = nixpkgsFor.${system};in
        {
          default = pkgs.callPackage ./shell.nix {};
          rust-shell = pkgs.callPackage nix/rust-shell.nix {};
          manage-shell = pkgs.callPackage nix/manage-shell.nix {};
          python-task-shell = pkgs.callPackage nix/python-task-shell.nix {};
        });
    }
  );
}
