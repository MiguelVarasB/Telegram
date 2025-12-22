"""
Gestión de conexiones WebSocket para carpetas.
"""
from typing import Dict, List
from fastapi import WebSocket


class FolderWSManager:
    """Gestiona conexiones WebSocket por carpeta para enviar avisos de refresco."""

    def __init__(self) -> None:
        self.connections: Dict[int, List[WebSocket]] = {}

    async def connect(self, folder_id: int, websocket: WebSocket) -> None:
        """Acepta y registra una conexión WebSocket."""
        await websocket.accept()
        self.connections.setdefault(folder_id, []).append(websocket)

    def disconnect(self, folder_id: int, websocket: WebSocket) -> None:
        """Elimina una conexión WebSocket del registro."""
        conns = self.connections.get(folder_id)
        if not conns:
            return
        if websocket in conns:
            conns.remove(websocket)
        if not conns:
            self.connections.pop(folder_id, None)

    async def broadcast_refresh(self, folder_id: int) -> None:
        """Envía una señal de 'refresh' a todos los clientes de esa carpeta."""
        conns = list(self.connections.get(folder_id, []))
        for ws in conns:
            try:
                await ws.send_json({"type": "refresh"})
            except Exception:
                self.disconnect(folder_id, ws)


# Instancia global del manager
ws_manager = FolderWSManager()
