{enable, description, cmd, user, ...}:
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
}
