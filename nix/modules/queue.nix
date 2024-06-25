{ lib, config, pkgs, ... }:
let parseQueueOptions = import ./parseQueueOptions.nix lib;
    vl-queue-definition = import ./vl-queue-definition.nix lib;
    mkQueue = import ./mkQueue.nix;
    cfg = config.services.vl-queue; in
{
  options.services.vl-queue = with lib;
    mkOption {
      description = "Vectorlink queue definitions";
      type = types.attrsOf vl-queue-definition;
      default = {};
    };

  config =
    let parsedCfg = lib.mapAttrs parseQueueOptions cfg; in
    {
      systemd.services = lib.mapAttrs (_: mkQueue) parsedCfg;
    };
}
