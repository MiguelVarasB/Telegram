# Guía de Migración a MQTT

## Resumen

Se ha migrado el sistema de notificaciones de WebSockets a MQTT usando el broker Mosquitto. Esta migración proporciona:

- ✅ Mayor escalabilidad y confiabilidad
- ✅ Soporte para múltiples clientes simultáneos
- ✅ Persistencia de mensajes (opcional)
- ✅ QoS (Quality of Service) configurable
- ✅ Compatibilidad con el hardware existente (Xeon + RTX 3050)

## Cambios Realizados

### Backend

#### 1. **Nuevo módulo: `utils/mqtt_manager.py`**
- Cliente MQTT usando `paho-mqtt` v2.0+
- Gestión de conexión asíncrona
- Métodos helper para publicar eventos comunes:
  - `publish_folder_refresh(folder_id)`
  - `publish_download_progress(...)`
  - `publish_video_visibility(...)`
  - `publish_scan_progress(...)`

#### 2. **Actualización de `config.py`**
Nuevas variables de configuración:
```python
MQTT_ENABLED = True  # Habilitar/deshabilitar MQTT
MQTT_BROKER = "127.0.0.2"  # IP del broker
MQTT_PORT = 1883  # Puerto MQTT estándar
MQTT_CLIENT_ID = "megatelegram_server"
MQTT_USERNAME = None  # Opcional
MQTT_PASSWORD = None  # Opcional
MQTT_KEEPALIVE = 60
```

#### 3. **Integración en `app.py`**
- MQTT Manager se inicializa en el `lifespan` de FastAPI
- Conexión automática al startup
- Desconexión limpia al shutdown

#### 4. **Actualización de `routes/folders.py`**
- Función `_broadcast_progress()` ahora publica vía MQTT además de WebSocket
- Tópicos MQTT:
  - `bot/folder/{folder_id}/scan` - Progreso de escaneo
  - `bot/folder/{folder_id}/refresh` - Señal de refresco

#### 5. **Actualización de `routes/media_stream.py`**
- Progreso de descargas se publica vía MQTT
- Tópico: `bot/video/download/progress`
- Estados: `downloading`, `completed`, `cancelled`, `failed`

### Frontend

#### 1. **Nuevo módulo: `static/js/app/mqtt_client.js`**
- Cliente MQTT para navegador usando Paho MQTT sobre WebSockets
- Gestión de conexión y reconexión automática
- Sistema de suscripciones con callbacks

#### 2. **Nuevo módulo: `static/js/app/mqtt_integration.js`**
- Integra MQTT con la lógica existente de la aplicación
- Mantiene compatibilidad con código legacy
- Maneja eventos de:
  - Escaneo de carpetas
  - Progreso de descargas
  - Cambios de visibilidad

## Pasos Pendientes para Completar la Migración

### 1. Instalar y Configurar Mosquitto

#### En Windows:

```powershell
# Descargar Mosquitto desde https://mosquitto.org/download/
# O usar Chocolatey:
choco install mosquitto

# Editar C:\Program Files\mosquitto\mosquitto.conf
```

#### Configuración de `mosquitto.conf`:

```conf
# Puerto MQTT estándar
listener 1883 127.0.0.2

# Puerto WebSocket para el frontend
listener 9001 127.0.0.2
protocol websockets

# Permitir conexiones anónimas (para desarrollo local)
allow_anonymous true

# Logs (opcional)
log_dest file C:/Program Files/mosquitto/mosquitto.log
log_type all

# Persistencia (opcional)
persistence true
persistence_location C:/Program Files/mosquitto/data/
```

#### Iniciar Mosquitto:

```powershell
# Como servicio
net start mosquitto

# O manualmente
mosquitto -c "C:\Program Files\mosquitto\mosquitto.conf" -v
```

### 2. Instalar Dependencias de Python

```bash
pip install paho-mqtt
```

### 3. Actualizar Templates HTML

Agregar los scripts MQTT al archivo `templates/partials/scripts.html`:

```html
<!-- Paho MQTT Client (CDN) -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/paho-mqtt/1.1.0/paho-mqtt.min.js"></script>

<!-- Cliente MQTT personalizado -->
<script src="/static/js/app/mqtt_client.js?v={{ V }}"></script>

<!-- Integración MQTT -->
<script src="/static/js/app/mqtt_integration.js?v={{ V }}"></script>
```

**Ubicación**: Después de `jquery` y antes de `core.js`

### 4. Configurar Variables de Entorno (Opcional)

Agregar al archivo `.env`:

```env
# MQTT Configuration
MQTT_ENABLED=1
MQTT_BROKER=127.0.0.2
MQTT_PORT=1883
MQTT_CLIENT_ID=megatelegram_server
# MQTT_USERNAME=usuario  # Opcional
# MQTT_PASSWORD=password  # Opcional
```

### 5. Verificar Funcionamiento

#### Test del Backend:

```bash
# Iniciar la aplicación
python app.py

# Verificar logs:
# ✅ MQTT Manager inicializado correctamente
```

#### Test del Frontend:

1. Abrir DevTools (F12) en el navegador
2. Ir a la pestaña Console
3. Verificar logs:
   ```
   [MQTT] Inicializando cliente: megatelegram_web_xxxxx
   [MQTT] Conectando a 127.0.0.2:9001...
   [MQTT] ✅ Conectado al broker
   [MQTT Integration] Cliente conectado, configurando suscripciones...
   ```

#### Test de Publicación:

```bash
# Usar mosquitto_pub para probar
mosquitto_pub -h 127.0.0.2 -t "bot/folder/1/refresh" -m '{"type":"refresh","folder_id":1}'
```

### 6. Monitoreo y Debug

#### Suscribirse a todos los tópicos (debug):

```bash
mosquitto_sub -h 127.0.0.2 -t "bot/#" -v
```

#### Ver logs de Mosquitto:

```powershell
Get-Content "C:\Program Files\mosquitto\mosquitto.log" -Wait
```

## Tópicos MQTT Implementados

| Tópico | QoS | Descripción | Payload |
|--------|-----|-------------|---------|
| `bot/folder/{id}/scan` | 1 | Progreso de escaneo de carpeta | `{type, job_id, status, done, total, running}` |
| `bot/folder/{id}/refresh` | 0 | Señal de refresco de carpeta | `{type: "refresh", folder_id}` |
| `bot/video/download/progress` | 1 | Progreso de descarga de video | `{type, chat_id, message_id, video_id, status, current, total, speed, eta}` |
| `bot/video/status/visibility` | 1 | Cambio de visibilidad de video | `{type, video_id, oculto, action}` |
| `bot/channel/{id}/scan` | 1 | Progreso de escaneo de canal | `{type, chat_id, status, current, total, message}` |

## Compatibilidad con WebSockets

**Importante**: El sistema mantiene compatibilidad con WebSockets existentes. Ambos sistemas funcionan en paralelo:

- **WebSocket**: Sigue funcionando para clientes legacy
- **MQTT**: Nuevo sistema para mayor escalabilidad

Para desactivar WebSockets completamente (futuro):
1. Comentar `@router.websocket("/ws/folder/{folder_id}")` en `routes/folders.py`
2. Remover `ws_manager.broadcast_event()` de `_broadcast_progress()`

## Troubleshooting

### Error: "MQTT Manager no pudo conectarse"

**Causa**: Mosquitto no está corriendo o no está escuchando en `127.0.0.2:1883`

**Solución**:
```powershell
# Verificar que Mosquitto está corriendo
Get-Service mosquitto

# Verificar puertos
netstat -an | findstr "1883"
netstat -an | findstr "9001"
```

### Error: "WebSocket connection failed" en el navegador

**Causa**: Puerto 9001 no está configurado en Mosquitto

**Solución**: Agregar al `mosquitto.conf`:
```conf
listener 9001 127.0.0.2
protocol websockets
```

### Error: "Connection refused" desde el frontend

**Causa**: Firewall de Windows bloqueando el puerto 9001

**Solución**:
```powershell
# Agregar regla de firewall
New-NetFirewallRule -DisplayName "Mosquitto WebSocket" -Direction Inbound -LocalPort 9001 -Protocol TCP -Action Allow
```

### Los mensajes no llegan al frontend

**Causa**: Cliente no suscrito correctamente

**Debug**:
1. Abrir DevTools Console
2. Verificar: `window.getMQTTClient().isConnected()`
3. Verificar suscripciones: `window.getMQTTClient().subscriptions`

## Próximos Pasos (Opcional)

### 1. Autenticación MQTT
```conf
# mosquitto.conf
allow_anonymous false
password_file C:/Program Files/mosquitto/passwd
```

```bash
# Crear usuario
mosquitto_passwd -c passwd megatelegram
```

### 2. TLS/SSL
```conf
listener 8883
cafile C:/certs/ca.crt
certfile C:/certs/server.crt
keyfile C:/certs/server.key
```

### 3. Persistencia de Mensajes
```conf
persistence true
persistence_location C:/Program Files/mosquitto/data/
autosave_interval 300
```

### 4. Límites de Rate
```conf
max_connections 100
max_inflight_messages 20
max_queued_messages 1000
```

## Referencias

- **Paho MQTT Python**: https://pypi.org/project/paho-mqtt/
- **Paho MQTT JavaScript**: https://www.eclipse.org/paho/index.php?page=clients/js/index.php
- **Mosquitto**: https://mosquitto.org/documentation/
- **MQTT Protocol**: https://mqtt.org/

## Soporte

Para problemas o dudas sobre la migración, revisar:
1. Logs de Mosquitto: `C:\Program Files\mosquitto\mosquitto.log`
2. Logs de la aplicación: Salida de `python app.py`
3. DevTools Console del navegador (F12)
