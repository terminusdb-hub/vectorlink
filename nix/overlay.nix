# overlay with all packages, as well as the build support tooling for building them.
final: prev:
let vectorlink-build-support = final.callPackage ./build {}; in
{
  vectorlink = {
    vectorlink = final.callPackage ../vectorlink {inherit vectorlink-build-support;};
    vectorlink-task-monitor = final.callPackage ../vectorlink-task-monitor {inherit vectorlink-build-support;};
    vectorlink-task-py = final.callPackage ../vectorlink-task-py {inherit vectorlink-build-support;};
    vectorlink-worker = final.callPackage ../vectorlink-worker {inherit vectorlink-build-support;};
    task-util = final.callPackage ../task-util {inherit vectorlink-build-support;};
    line-index = final.callPackage ../line-index {inherit vectorlink-build-support;};
    vectorlink-vectorize = final.callPackage ../python/vectorlink-vectorize {};
    create-vectorize-tasks = final.callPackage ../python/create-vectorize-task {};
    read-line-from-index = final.callPackage ../python/read-line-from-index {};
  };

  # vectorlink-modules.vl-queue = import modules/queue.nix;
}
