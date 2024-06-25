# Default build parameters for feeding into craneLib
{craneLib, pkg-config, protobuf, python311, openssl, stdenv}:
_: # stupid trick to prevent output of this 'package' to be interpreted as a derivation
let rustFlagsFor = {
      x86 = "-C target-feature=+sse3,+avx,+avx2";
      arm = "-C target-feature=+neon";
    };
in
{
  nativeBuildInputs = [
    pkg-config
    protobuf
    python311
  ];
  buildInputs = [
    openssl
  ];
  RUSTFLAGS = if stdenv.hostPlatform.isAarch64 then rustFlagsFor.arm else rustFlagsFor.x86;
  src = craneLib.cleanCargoSource (craneLib.path ../..);
  strictDeps = true;
  doCheck = false;
}
