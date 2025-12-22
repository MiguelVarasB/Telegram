import asyncio

import reenviar_videos
import descargar_dump


async def main():
    ciclo = 0
    while True:
        ciclo += 1
        print(f"\n{'=' * 16} CICLO {ciclo} / REENVIAR {'=' * 16}")
        res_reenviar = await reenviar_videos.main()
        reenviados = int(res_reenviar.get("reenviados", 0) or 0)
        pendientes_reenviar = int(res_reenviar.get("pendientes", 0) or 0)
        print(f"[Flujo] Reenviar: pendientes={pendientes_reenviar} reenviados={reenviados}")

        print(f"\n{'=' * 16} CICLO {ciclo} / DESCARGAR {'=' * 16}")
        res_descargar = await descargar_dump.main(recycle_when_all_floodwait=True)
        recycled = bool(res_descargar.get("recycled", False))
        tareas_iniciales = int(res_descargar.get("tareas_iniciales", 0) or 0)
        descargas = int(res_descargar.get("descargas", 0) or 0)
        errores = int(res_descargar.get("errores", 0) or 0)
        print(
            f"[Flujo] Descargar: tareas_iniciales={tareas_iniciales} descargas={descargas} errores={errores} recycled={recycled}"
        )

        if recycled:
            print("[Flujo] Todos los bots en FLOODWAIT. Volviendo a REENVIAR...")
            continue

        if reenviados == 0 and tareas_iniciales == 0:
            print("[Flujo] Nada para hacer. Esperando 60s...")
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
