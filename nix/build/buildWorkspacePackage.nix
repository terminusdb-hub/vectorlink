{ craneLib, vl-workspace, rust-args }:
{projectPath,...}@args:
let cargoToml = projectPath + "/Cargo.toml";
    nameInfo = craneLib.crateNameFromCargoToml {inherit cargoToml;};
in
craneLib.buildPackage (rust-args // nameInfo // {
  cargoArtifacts = vl-workspace;
  cargoExtraArgs = "-p " + nameInfo.pname;
} // args)
