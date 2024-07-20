{
  description = ''zig-iobuf - A Zig buffer for easier zero-copy networking programming.'';

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    flake-utils.url = "github:numtide/flake-utils";

    pre-commit-hooks.url = "github:cachix/pre-commit-hooks.nix";
    pre-commit-hooks.inputs.nixpkgs.follows = "nixpkgs";

    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";

    zig-overlay.url = "github:mitchellh/zig-overlay";
    zig-overlay.inputs.nixpkgs.follows = "nixpkgs";
    zig-overlay.inputs.flake-utils.follows = "flake-utils";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    devshell,
    pre-commit-hooks,
    zig-overlay,
    ...
  }: let
    systems = with flake-utils.lib.system; [
      x86_64-linux
      x86_64-darwin
      aarch64-darwin
    ];

    systemAttrs = flake-utils.lib.eachSystem systems (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [devshell.overlays.default];
      };

      pre-commit-check = pre-commit-hooks.lib.${system}.run {
        src = ./.;
        hooks = {
          alejandra.enable = true;
          deadnix.enable = true;
          trim-trailing-whitespace.enable = true;
          trim-trailing-whitespace.excludes = ["mimalloc/"];

          zig-fmt = {
            enable = true;
            entry = "zig fmt";
            files = "\\.zig$";
          };

          zig-check = {
            enable = true;
            entry = "zig build check";
            pass_filenames = false;
            files = "\\.zig$";
          };
        };
      };

      devShell = let
        attrsToCommand = attrs: map (name: attrs.${name} // {inherit name;}) (builtins.attrNames attrs);
      in
        pkgs.devshell.mkShell {
          name = "zig-iobuf";

          commands = attrsToCommand {
            c = {
              help = "Run repository checks";
              command = "pre-commit run -a";
              category = "checks";
            };
          };

          devshell.startup.enable-pre-commit.text = pre-commit-check.shellHook;

          packages = with pkgs; [
            zig-overlay.packages.${system}.master
            pkg-config
            gdb
          ];
        };
    in {
      checks = {inherit pre-commit-check;};

      devShells.default = devShell;
    });
  in
    systemAttrs
    // {
      lib = import ./lib {nixpkgsLib = nixpkgs.lib;};
    };
}
