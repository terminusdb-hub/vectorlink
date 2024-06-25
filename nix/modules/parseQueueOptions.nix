lib:
worker-name:
{ enable, description, bin, etcd, service-name, identity-method, extra-args, user, ... }:
let service-arg = if service-name != null then "--service ${service-name}"  else "";
    etcd-arg = lib.concatStringsSep "," etcd;
    extra-args' = lib.concatStringsSep " " extra-args;
    user' = if user != null then user else worker-name;
in
{
  inherit enable;
  inherit description;
  user = user';
  cmd = "${bin} ${service-arg} ${etcd-arg} ${extra-args'}";
}
