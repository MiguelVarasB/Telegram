import os
import subprocess
import sys

def _project_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))

def _cli_dir() -> str:
    return os.path.join(_project_root(), "CLI")

def _list_cli_scripts(cli_dir: str) -> list[str]:
    if not os.path.isdir(cli_dir):
        return []

    items: list[str] = []
    for name in os.listdir(cli_dir):
        if not name.lower().endswith(".py"):
            continue
        if name.startswith("__"):
            continue
        items.append(name)

    items.sort(key=lambda s: s.lower())
    return items

def _choose_script(scripts: list[str]) -> str | None:
    if not scripts:
        return None

    print("\nScripts disponibles en CLI/:")
    for i, s in enumerate(scripts, start=1):
        print(f"{i}) {s}")

    print("\nElige un número y presiona ENTER (o 'q' para salir).")
    while True:
        raw = input("> ").strip()
        if raw.lower() in {"q", "quit", "exit"}:
            return None
        if not raw:
            continue
        try:
            idx = int(raw)
        except ValueError:
            print("Entrada inválida. Escribe un número o 'q'.")
            continue
        if 1 <= idx <= len(scripts):
            return scripts[idx - 1]
        print(f"Número fuera de rango. Debe ser 1..{len(scripts)}")

def main() -> int:
    cli_dir = _cli_dir()
    scripts = _list_cli_scripts(cli_dir)

    if not scripts:
        print(f"No se encontraron scripts .py en: {cli_dir}")
        return 1

    selected = _choose_script(scripts)
    if not selected:
        print("Cancelado.")
        return 0

    script_path = os.path.join(cli_dir, selected)
    print(f"\nEjecutando: {script_path}\n")

    # Ejecuta con el mismo Python que ejecutó este launcher
    cmd = [sys.executable, script_path]
    try:
        return subprocess.call(cmd, cwd=_project_root())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
