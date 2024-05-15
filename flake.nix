{
  description = "Vectorlink projects and machines";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-23.11";
    nixpkgs-unstable.url = "github:nixos/nixpkgs?ref=nixos-unstable";
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

  outputs = { self, nixpkgs, nixpkgs-unstable, crane, rust-overlay }@inputs: (
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
            unstablepkgs = import nixpkgs-unstable {
              inherit system;
              config = {
                allowUnfree = true;
                cudaSupport = true;
                cudaVersion = "12";
              };
            };
        in
        {
          vectorlink = pkgs.callPackage ./vectorlink {};
          vectorlink-worker = pkgs.callPackage ./vectorlink-worker {};
          line-index = pkgs.callPackage ./line-index {};
          snowflake-concat = pkgs.callPackage ./snowflake-concat {};
          vectorlink-infra = pkgs.callPackage ./vectorlink-infra {};
          vectorlink-task-monitor = unstablepkgs.callPackage python/vectorlink-task {
            config = { allowUnfree = true;
                       cudaSupport = true;
                     };
          };
          vectorlink-vectorize = unstablepkgs.callPackage python/vectorlink-vectorize {};
        }
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
          backend = {
            type = "app";
            program = "${p.vectorlink-vectorize}/bin/backend";
          };
        });
    }
  );
}
