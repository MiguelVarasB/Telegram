import os
import subprocess
import sys
from typing import List, Tuple

# Entradas que no se mostrarán en el menú (carpetas o scripts).
IGNORE_ENTRIES: list[str] = []
IGNORE_ENTRIES = [".old"]

def _project_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _cli_dir() -> str:
    return os.path.join(_project_root(), "CLI")


def _list_entries(path: str) -> Tuple[List[str], List[str]]:
    """Devuelve (subcarpetas, scripts .py) dentro de path, ordenados."""
    if not os.path.isdir(path):
        return [], []

    dirs: List[str] = []
    scripts: List[str] = []
    for name in os.listdir(path):
        if name.startswith("__") or name in IGNORE_ENTRIES:
            continue
        full = os.path.join(path, name)
        if os.path.isdir(full):
            dirs.append(name)
        elif name.lower().endswith(".py"):
            scripts.append(name)

    dirs.sort(key=lambda s: s.lower())
    scripts.sort(key=lambda s: s.lower())
    return dirs, scripts


def _choose_script(cli_dir: str) -> str | None:
    """Permite navegar subcarpetas dentro de CLI y elegir un script .py."""
    stack: List[str] = []

    while True:
        current_dir = os.path.join(cli_dir, *stack) if stack else cli_dir
        rel = os.path.relpath(current_dir, cli_dir)
        rel_display = "." if rel == "." else rel

        dirs, scripts = _list_entries(current_dir)
        if not dirs and not scripts:
            print(f"\n(No hay entradas en {rel_display})")
        else:
            print(f"\nCarpeta CLI/{rel_display}:")
            entries: List[Tuple[str, str, bool]] = []
            for d in dirs:
                entries.append((d, os.path.join(current_dir, d), True))
            for s in scripts:
                entries.append((s, os.path.join(current_dir, s), False))

            for i, (name, _, is_dir) in enumerate(entries, start=1):
                prefix = "[dir]" if is_dir else "     "
                print(f"{i:2}) {prefix} {name}")

        back_allowed = bool(stack)
        print("\nElige número y ENTER ( 'q' para salir"
              + (" | 'b' para retroceder" if back_allowed else "")
              + " ).")

        raw = input("> ").strip().lower()
        if raw in {"q", "quit", "exit"}:
            return None
        if raw in {"b", "back"}:
            if back_allowed:
                stack.pop()
            else:
                print("Ya estás en la raíz.")
            continue
        if not raw:
            continue
        try:
            idx = int(raw)
        except ValueError:
            print("Entrada inválida. Usa número, 'b' o 'q'.")
            continue

        if not dirs and not scripts:
            print("No hay nada para seleccionar.")
            continue

        entries: List[Tuple[str, str, bool]] = []
        for d in dirs:
            entries.append((d, os.path.join(current_dir, d), True))
        for s in scripts:
            entries.append((s, os.path.join(current_dir, s), False))

        if 1 <= idx <= len(entries):
            name, path, is_dir = entries[idx - 1]
            if is_dir:
                stack.append(name)
                continue
            return path

        print(f"Número fuera de rango. Debe ser 1..{len(entries)}")


def main() -> int:
    cli_dir = _cli_dir()
    script_path = _choose_script(cli_dir)

    if not script_path:
        print("Cancelado.")
        return 0

    print(f"\nEjecutando: {script_path}\n")

    # Ejecuta con el mismo Python que ejecutó este launcher
    cmd = [sys.executable, script_path]
    env = os.environ.copy()
    root = _project_root()
    env["PYTHONPATH"] = root + os.pathsep + env.get("PYTHONPATH", "")
    try:
        return subprocess.call(cmd, cwd=root, env=env)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
