{ lib, config, pkgs, ... }:
let vl-queue-definition = import ./vl-queue-definition.nix lib;
    cfg = config.services.vectorlink-worker; in
{
  options.services.vectorlink-worker = with lib;
    mkOption {
      description = "Vectorlink worker configuration";
      type = vl-queue-definition;
      default = {};
    };

  config =
    {
      services.vl-queue.vectorlink-worker = cfg // {
          description = "HNSW indexer";
          bin = "${pkgs.vectorlink.vectorlink-worker}/bin/vectorlink-worker";
      };
    };
}
