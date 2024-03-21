 let
   nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-23.11";
   pkgs = import nixpkgs { config = {}; overlays = []; };
 in

 pkgs.mkShellNoCC {
   packages = with pkgs; [
     python39
     poetry
   ];

   GREETING = "Hello, Nix!";

   shellHook = ''
     # poetry shell
     poetry shell
   '';
 }
