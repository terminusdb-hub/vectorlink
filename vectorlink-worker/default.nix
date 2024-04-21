{ naersk, pkg-config, openssl, protobuf }:
naersk.buildPackage {
  nativeBuildInputs = [
    pkg-config
  ];
  buildInputs = [
    openssl
    protobuf
  ];

  src = ./.;
  root = ../.;
}
