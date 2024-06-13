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
            /*
            unstablepkgs = import nixpkgs-unstable {
              inherit system;
              config = {
                allowUnfree = true;
                cudaSupport = true;
                cudaVersion = "12";
              };
            };
*/
        in
        {
          vectorlink = pkgs.callPackage ./vectorlink {};
          vectorlink-task-monitor = pkgs.callPackage ./vectorlink-task-monitor {};
          vectorlink-task-py = pkgs.callPackage ./vectorlink-task-py {};
          vectorlink-worker = pkgs.callPackage ./vectorlink-worker {};
          task-util = pkgs.callPackage ./task-util {};
          line-index = pkgs.callPackage ./line-index {};
          vectorlink-vectorize = pkgs.callPackage python/vectorlink-vectorize {};
          create-vectorize-tasks = pkgs.callPackage python/create-vectorize-task {};
          read-line-from-index = pkgs.callPackage python/read-line-from-index {};
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
        });

      devShells = forAllSystems (system :
        let pkgs = nixpkgsFor.${system};in
        {
          rust-shell = pkgs.callPackage nix/rust-shell.nix {};
          manage-shell = pkgs.callPackage nix/manage-shell.nix {};
          python-task-shell = pkgs.callPackage nix/python-task-shell.nix {};
        });
    }
  );
}
