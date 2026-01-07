"""
Gestor de conexiones MQTT para notificaciones en tiempo real.
Reemplaza WebSockets con un broker Mosquitto local.
"""
import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional
from paho.mqtt import client as mqtt_client

logger = logging.getLogger(__name__)


class MQTTManager:
    """
    Gestor centralizado de MQTT para publicar eventos del sistema.
    Compatible con paho-mqtt v2.0+
    """

    def __init__(
        self,
        broker: str = "127.0.0.2",
        port: int = 1883,
        client_id: str = "megatelegram_server",
        username: Optional[str] = None,
        password: Optional[str] = None,
        keepalive: int = 60,
    ):
        self.broker = broker
        self.port = port
        self.client_id = client_id
        self.username = username
        self.password = password
        self.keepalive = keepalive
        
        self.client: Optional[mqtt_client.Client] = None
        self.connected = False
        self._reconnect_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback cuando se conecta al broker (API v2.0+)."""
        if reason_code == 0:
            self.connected = True
            logger.info(f"✅ MQTT conectado al broker {self.broker}:{self.port}")
        else:
            self.connected = False
            logger.error(f"❌ MQTT falló la conexión: código {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        """Callback cuando se desconecta del broker (API v2.0+)."""
        self.connected = False
        logger.warning(f"⚠️ MQTT desconectado: código {reason_code}")
        
    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """Callback cuando se publica un mensaje (API v2.0+)."""
        if reason_code != 0:
            logger.warning(f"⚠️ MQTT publicación falló: mid={mid}, código={reason_code}")

    async def connect(self) -> bool:
        """
        Conecta al broker MQTT de forma asíncrona.
        Retorna True si la conexión fue exitosa.
        """
        try:
            self._loop = asyncio.get_event_loop()
            
            # Crear cliente MQTT v2.0+ con CallbackAPIVersion
            self.client = mqtt_client.Client(
                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
                client_id=self.client_id,
                protocol=mqtt_client.MQTTv311,
            )
            
            # Configurar callbacks
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_publish = self._on_publish
            
            # Autenticación si se proporciona
            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)
            
            # Conectar de forma no bloqueante
            self.client.connect_async(self.broker, self.port, self.keepalive)
            self.client.loop_start()
            
            # Esperar conexión (timeout 5s)
            for _ in range(50):
                if self.connected:
                    logger.info("🔌 MQTT Manager inicializado correctamente")
                    return True
                await asyncio.sleep(0.1)
            
            logger.warning("⏱️ MQTT timeout esperando conexión")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error conectando MQTT: {e}")
            self.connected = False
            return False

    async def disconnect(self):
        """Desconecta del broker MQTT limpiamente."""
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
                logger.info("🔌 MQTT desconectado limpiamente")
            except Exception as e:
                logger.error(f"❌ Error desconectando MQTT: {e}")
        
        self.connected = False
        self.client = None

    def publish(
        self,
        topic: str,
        payload: Any,
        qos: int = 0,
        retain: bool = False,
    ) -> bool:
        """
        Publica un mensaje en un tópico MQTT.
        
        Args:
            topic: Tópico MQTT (ej: "bot/video/download/progress")
            payload: Datos a enviar (dict, str, bytes)
            qos: Quality of Service (0, 1, 2)
            retain: Si el mensaje debe ser retenido por el broker
            
        Returns:
            True si se publicó exitosamente
        """
        if not self.connected or not self.client:
            logger.warning(f"⚠️ MQTT no conectado, no se puede publicar en '{topic}'")
            return False
        
        try:
            # Serializar payload si es dict
            if isinstance(payload, dict):
                payload = json.dumps(payload, ensure_ascii=False)
            elif not isinstance(payload, (str, bytes)):
                payload = str(payload)
            
            # Publicar
            result = self.client.publish(topic, payload, qos=qos, retain=retain)
            
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                logger.debug(f"📤 MQTT publicado en '{topic}': {payload[:100]}")
                return True
            else:
                logger.warning(f"⚠️ MQTT error publicando en '{topic}': rc={result.rc}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error publicando MQTT en '{topic}': {e}")
            return False

    async def publish_async(
        self,
        topic: str,
        payload: Any,
        qos: int = 0,
        retain: bool = False,
    ) -> bool:
        """
        Versión asíncrona de publish() para uso en contextos async.
        Ejecuta la publicación en el executor del loop.
        """
        if not self._loop:
            self._loop = asyncio.get_event_loop()
        
        return await self._loop.run_in_executor(
            None, self.publish, topic, payload, qos, retain
        )

    def publish_folder_refresh(self, folder_id: int) -> bool:
        """Publica evento de refresco de carpeta."""
        return self.publish(
            topic=f"bot/folder/{folder_id}/refresh",
            payload={"type": "refresh", "folder_id": folder_id},
            qos=0,
        )

    def publish_download_progress(
        self,
        chat_id: int,
        message_id: int,
        video_id: str,
        status: str,
        current: int,
        total: int,
        speed: float = 0,
        eta: Optional[int] = None,
    ) -> bool:
        """Publica progreso de descarga de video."""
        payload = {
            "type": "download_progress",
            "chat_id": chat_id,
            "message_id": message_id,
            "video_id": video_id,
            "status": status,
            "current": current,
            "total": total,
            "speed": speed,
            "eta": eta,
        }
        return self.publish(
            topic="bot/video/download/progress",
            payload=payload,
            qos=1,
        )

    def publish_video_visibility(
        self,
        video_id: int,
        oculto: int,
        action: str = "updated",
    ) -> bool:
        """Publica cambio de visibilidad de video."""
        payload = {
            "type": "video_visibility",
            "video_id": video_id,
            "oculto": oculto,
            "action": action,
        }
        return self.publish(
            topic="bot/video/status/visibility",
            payload=payload,
            qos=1,
        )

    def publish_scan_progress(
        self,
        chat_id: int,
        status: str,
        current: int,
        total: int,
        message: Optional[str] = None,
    ) -> bool:
        """Publica progreso de escaneo de canal."""
        payload = {
            "type": "scan_progress",
            "chat_id": chat_id,
            "status": status,
            "current": current,
            "total": total,
            "message": message,
        }
        return self.publish(
            topic=f"bot/channel/{chat_id}/scan",
            payload=payload,
            qos=1,
        )

    def is_connected(self) -> bool:
        """Verifica si el cliente está conectado."""
        return self.connected and self.client is not None


# Instancia global del manager
mqtt_manager: Optional[MQTTManager] = None


def get_mqtt_manager() -> Optional[MQTTManager]:
    """Obtiene la instancia global del MQTT Manager."""
    return mqtt_manager


def init_mqtt_manager(
    broker: str = "127.0.0.2",
    port: int = 1883,
    client_id: str = "megatelegram_server",
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> MQTTManager:
    """
    Inicializa la instancia global del MQTT Manager.
    Debe llamarse durante el startup de la aplicación.
    """
    global mqtt_manager
    mqtt_manager = MQTTManager(
        broker=broker,
        port=port,
        client_id=client_id,
        username=username,
        password=password,
    )
    return mqtt_manager
