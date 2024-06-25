{ pkgs,
  callPackage,
  rust-args ? callPackage ./rust-args.nix {} 0,
}:
rec {
  inherit rust-args;
  vl-workspace = callPackage ./vl-workspace.nix { inherit rust-args; };
  buildWorkspacePackage = callPackage ./buildWorkspacePackage.nix {
    inherit vl-workspace;
    inherit rust-args;
  };
  buildMaturinWorkspacePackage = callPackage ./buildMaturinWorkspacePackage.nix {
    inherit buildWorkspacePackage;
  };
}
