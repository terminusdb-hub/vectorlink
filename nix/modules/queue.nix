let mkParseQueueOptions = import ./parseQueueOptions.nix; in
{ lib, config, pkgs, ... }:
let cfg = config.services.vl-queue; in
{
  options.services.vl-queue = with lib;
    mkOption {
      description = "Vectorlink queue definitions";
      type = types.attrsOf (types.submodule
        {
          options = {
            enable = mkEnableOption "Enable this queue";
            bin = mkOption {
              description = "The binary to use for this queue";
              type = types.path;
            };
            description = mkOption {
              description = "service description";
              type = types.str;
            };
            etcd = mkOption {
              description = "etcd endpoints";
              type = types.listOf types.str;
            };
            service-name = mkOption {
              description = "Override the default queue the binary would listen on with the given service name";
              type = types.nullOr types.str;
              default = null;
            };
            identity-method = mkOption {
              description = "Method to determine the identity we should use when picking up queue items.";
              type = types.enum ["worker" "ip" "static"];
              default = "ip";
            };
            extra-args = mkOption {
              description = "extra arguments to pass to the worker";
              type = types.listOf types.str;
              default = [];
            };
            user = mkOption {
              description = "The user to run this service as. Defaults to a username derived from the service name.";
              type = types.nullOr types.str;
              default = null;
            };
          };
        });
      default = {};
    };

  config =
    let parseQueueOptions = mkParseQueueOptions lib;
        parsedCfg = lib.mapAttrs parseQueueOptions cfg;
    in
      {
        systemd.services = lib.mapAttrs (_: {enable, description, cmd, user, ...}:
          {
            inherit enable;

            wantedBy = ["multi-user.target"];
            after = [ "network-online.target" ];
            wants = [ "network-online.target" ];
            inherit description;
            restartIfChanged = true;
            serviceConfig = {
              Type = "simple";
              ExecStart = cmd;
              Restart = "always";
              RestartSec = "30s";
              User = user;
            };
          }) parsedCfg;
      };
}
