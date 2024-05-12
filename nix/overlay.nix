# Takes flake inputs as an attribute set, and a path to the workspace root.
# Output is a an overlay for # building the projects in this
# workspace.
#
# Specifically, we ensure there's a configured craneLib with
# arch-specific rust args for simd instructions, and we provide a
# workspace dependency derivation that all projects can then depend
# on.
let rustFlagsFor = {
      x86 = "-C target-feature=+sse3,+avx,+avx2";
      arm = "-C target-feature=+neon";
    };
in
path:
{nixpkgs, rust-overlay, crane, poetry2nix, ...}:
system:
import nixpkgs {
  inherit system;
  #config = { allowUnfree = true; cudaSupport = true; };
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
        RUSTFLAGS = if final.stdenv.hostPlatform.isAarch64 then rustFlagsFor.arm else rustFlagsFor.x86;
        src = craneLib.cleanCargoSource (craneLib.path path);
        strictDeps = true;
        doCheck = false;
      };
      vl-workspace = craneLib.buildDepsOnly (rust-args // {
        pname = "vectorlink";
        version = "0.1.0";
      });
      buildWorkspacePackage = {projectPath,...}@args:
        let cargoToml = projectPath + "/Cargo.toml";
            nameInfo = craneLib.crateNameFromCargoToml {inherit cargoToml;};
        in
          craneLib.buildPackage (rust-args // nameInfo // {
            cargoArtifacts = vl-workspace;
            cargoExtraArgs = "-p " + nameInfo.pname;
          } // args);
    })
  ];
}
