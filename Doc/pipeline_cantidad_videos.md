# Pipeline de Cantidad de Videos - Documentación Técnica

## Descripción General

El pipeline de cantidad de videos es un sistema automatizado que sincroniza, indexa y audita videos de Telegram almacenados en la base de datos local. El proceso está diseñado para mantener consistencia entre los datos de Telegram y la base de datos local, calculando métricas precisas sobre videos indexados y duplicados.

## Arquitectura de Base de Datos

### Tablas Principales

#### 1. `chats`
Almacena información de los chats (grupos, supergrupos, canales).

**Campos clave:**
- `chat_id`: Identificador único del chat
- `activo`: Indica si el chat está activo (1) o inactivo (0)
- `last_message_date`: Fecha del último mensaje en el chat (obtenida de Unigram)
- `ultimo_escaneo`: Timestamp del último escaneo realizado

#### 2. `chat_video_counts`
Almacena contadores y métricas por chat.

**Campos clave:**
- `chat_id`: Referencia al chat
- `videos_count`: Total de videos en la nube (Telegram API)
- `indexados`: Videos indexados en `video_messages`
- `duplicados`: Mensajes duplicados detectados en `video_messages`

#### 3. `videos_telegram`
Almacena información única de cada video.

**Campos clave:**
- `file_unique_id`: Identificador único del video (PK)
- `chat_id`: Chat donde se encontró el video
- `file_name`, `duration`, `width`, `height`, etc.

#### 4. `video_messages`
Almacena cada ocurrencia de un video en un mensaje.

**Campos clave:**
- `video_id`: Referencia a `videos_telegram.file_unique_id`
- `chat_id`: Chat del mensaje
- `message_id`: ID del mensaje en Telegram
- `date`: Fecha del mensaje
- `message_data`: JSON con metadata completa del mensaje

**Relación:** Un video en `videos_telegram` puede tener múltiples entradas en `video_messages` (mismo video compartido varias veces).

## Flujo del Pipeline

### Paso 0: Guardar/Actualizar Chats
**Archivo:** `CLI/cantidad_videos/guardar_chats.py`

**Objetivo:** Obtener todos los chats activos desde la API de Telegram y marcarlos como activos.

**Proceso:**
1. Resetea todos los chats a `activo=0` y `last_message_date=NULL`
2. Obtiene fechas confiables desde Unigram (base de datos de escritorio de Telegram)
3. Recorre todos los diálogos del usuario usando Pyrogram
4. Filtra solo grupos, supergrupos y canales
5. Para cada chat:
   - Obtiene `last_message_date` del diálogo
   - Si existe en Unigram, usa esa fecha (más confiable)
   - Marca el chat como `activo=1`
   - Guarda/actualiza en la tabla `chats`

**Resultado:** Solo los chats que aparecen en la API quedan con `activo=1`.

---

### Paso 1: Contar Videos en la Nube
**Archivo:** `CLI/cantidad_videos/auditar_conteo_videos_chats.py`

**Objetivo:** Obtener el conteo real de videos desde Telegram API y guardarlo en `chat_video_counts.videos_count`.

**Proceso:**
1. Selecciona chats activos (`activo=1`) que cumplan:
   - `ultimo_escaneo IS NULL` (nunca escaneados) **O**
   - `last_message_date > fecha_actual - UMBRAL_DIAS` (actividad reciente)
2. Para cada chat:
   - Usa `client.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO)` para contar videos
   - Guarda el conteo en `chat_video_counts.videos_count`
   - Actualiza `ultimo_escaneo` con timestamp actual

**Resultado:** `videos_count` refleja el total de videos en Telegram para cada chat activo.

---

### Paso 2: Recalcular Indexados desde BD
**Archivo:** `CLI/cantidad_videos/recalcular_indexados_desde_bd.py`

**Objetivo:** Contar cuántos mensajes con video están indexados en `video_messages` por chat.

**Proceso:**
1. Resetea `indexados=0` en todos los chats
2. Ejecuta query:
   ```sql
   SELECT chat_id, COUNT(*) 
   FROM video_messages 
   GROUP BY chat_id
   ```
3. Actualiza `chat_video_counts.indexados` con los conteos

**Resultado:** `indexados` refleja cuántos mensajes con video están en la BD local.

---

### Paso 3: Calcular Duplicados
**Archivo:** `CLI/pipeline_cantidad_videos.py` → `contar_duplicados_y_actualizar()`

**Objetivo:** Detectar mensajes duplicados (mismo `video_id` aparece múltiples veces en un chat).

**Proceso:**
1. Ejecuta query para encontrar duplicados:
   ```sql
   WITH dupes AS (
       SELECT chat_id, COUNT(*) AS c
       FROM (
           SELECT chat_id, video_id, COUNT(*) AS n
           FROM video_messages
           GROUP BY chat_id, video_id
           HAVING n > 1
       )
       GROUP BY chat_id
   )
   SELECT chat_id, c FROM dupes
   ```
2. Actualiza `chat_video_counts.duplicados` con el conteo

**Resultado:** `duplicados` indica cuántos mensajes duplicados existen por chat.

---

### Paso 4: Sincronizar Faltantes por Búsqueda
**Archivo:** `CLI/cantidad_videos/sincronizar_faltantes_search.py`

**Objetivo:** Buscar y descargar metadata de videos faltantes desde Telegram.

**Proceso:**
1. Selecciona chats activos (`activo=1`) donde `videos_count > indexados` (hay videos faltantes)
2. Para cada chat:
   - Busca mensajes con video usando `client.search_messages()`
   - Para cada mensaje encontrado:
     - Verifica si el mensaje ya existe en `video_messages` (por `chat_id` + `message_id`)
     - Si no existe:
       - Guarda el video en `videos_telegram` (si es nuevo)
       - Guarda el mensaje en `video_messages`
       - Incrementa contador de nuevos
     - Si existe:
       - Incrementa contador de consecutivos existentes
       - **Log detallado:** `ℹ️ Ya existe: video.mp4 | msg_id=497 | chat_id=-1001662968475 (consecutivos: 1/30)`
   - Detiene el chat si alcanza X consecutivos existentes (configurable)

**Resultado:** Videos recientes son indexados en la BD. Los logs muestran `msg_id` y `chat_id` para análisis.

---

### Paso 5: Indexar Histórico
**Archivo:** `CLI/cantidad_videos/indexador_historico.py`

**Objetivo:** Indexar videos antiguos retrocediendo desde el mensaje más antiguo conocido.

**Proceso:**
1. Selecciona chats activos con historial:
   ```sql
   SELECT cvc.chat_id, MIN(vm.message_id) as min_msg_id
   FROM chat_video_counts cvc
   JOIN video_messages vm ON cvc.chat_id = vm.chat_id
   JOIN chats c ON cvc.chat_id = c.chat_id
   WHERE c.activo = 1
   GROUP BY cvc.chat_id
   ```
2. Para cada chat:
   - Obtiene historial usando `client.get_chat_history()` desde `min_msg_id - 1` hacia atrás
   - Para cada mensaje con video:
     - Verifica si ya existe en BD
     - Si es nuevo:
       - Guarda video y mensaje
       - **Log:** `[HISTORIA] msg_id=78442 | chat_id=-1001326977142 | archivo.mp4`
     - Si existe:
       - **Log:** `[EXISTE] msg_id=78441 | chat_id=-1001326977142 | archivo.mp4 (Video ya conocido)`
   - Detiene si encuentra X consecutivos existentes (ya indexado)

**Resultado:** Videos históricos son indexados. Los logs incluyen `msg_id` y `chat_id` para trazabilidad.

---

### Paso 6: Recalcular Indexados (Post)
**Archivo:** `CLI/cantidad_videos/recalcular_indexados_desde_bd.py`

**Objetivo:** Actualizar contadores después de la sincronización.

**Proceso:** Igual que Paso 2.

---

### Paso 7: Recalcular Duplicados (Post)
**Archivo:** `CLI/pipeline_cantidad_videos.py` → `contar_duplicados_y_actualizar()`

**Objetivo:** Actualizar contadores de duplicados después de la sincronización.

**Proceso:** Igual que Paso 3.

---

## Diferencias Clave entre Tablas

### `videos_telegram` vs `video_messages`

| Aspecto | `videos_telegram` | `video_messages` |
|---------|-------------------|------------------|
| **Propósito** | Videos únicos | Ocurrencias de videos en mensajes |
| **PK** | `file_unique_id` | Compuesto: `chat_id` + `message_id` + `video_id` |
| **Cardinalidad** | 1 video = 1 fila | 1 video puede tener N filas (N mensajes) |
| **Uso en conteos** | ~~No se usa para `indexados`~~ | Se usa para `indexados` y `duplicados` |

### ¿Por qué contar desde `video_messages`?

- **`indexados`**: Representa cuántos **mensajes con video** están en la BD, no cuántos videos únicos.
- **`duplicados`**: Detecta cuando el mismo video (`video_id`) aparece en múltiples mensajes del mismo chat.

**Ejemplo:**
- Chat tiene 3 mensajes: msg1, msg2, msg3
- msg1 y msg2 comparten el mismo video (video_A)
- msg3 tiene otro video (video_B)

**Resultado:**
- `videos_telegram`: 2 filas (video_A, video_B)
- `video_messages`: 3 filas (msg1→video_A, msg2→video_A, msg3→video_B)
- `indexados`: 3 (total de mensajes)
- `duplicados`: 1 (video_A aparece 2 veces)

---

## Filtrado de Chats Inactivos

**Regla:** Todos los pasos que seleccionan chats deben filtrar por `activo=1`.

**Implementación:**
- Paso 1: `WHERE c.activo = 1 AND (c.ultimo_escaneo IS NULL OR c.last_message_date > ?)`
- Paso 4: `WHERE c.activo = 1 AND cvc.videos_count > cvc.indexados`
- Paso 5: `WHERE c.activo = 1`

**Razón:** Chats que no aparecen en la API de Telegram (eliminados, abandonados, etc.) quedan con `activo=0` y son ignorados.

---

## Logging Detallado

### Formato de Logs

**Paso 4 (Sincronización):**
```
ℹ️  Ya existe: video.mp4 | msg_id=497 | chat_id=-1001662968475 (consecutivos: 1/30)
```

**Paso 5 (Histórico):**
```
[HISTORIA] msg_id=78442 | chat_id=-1001326977142 | archivo.mp4
[EXISTE] msg_id=78441 | chat_id=-1001326977142 | archivo.mp4 (Video ya conocido)
```

**Propósito:** Facilitar análisis y debugging mostrando exactamente qué mensaje y chat está siendo procesado.

---

## Reporte JSON

Cada ejecución del pipeline genera un reporte JSON en:
```
pipeline_report_YYYYMMDDTHHMMSS.json
```

**Estructura:**
```json
{
  "steps": [
    {
      "step": "paso0_guardar_chats",
      "status": "ok",
      "timestamp": "2026-01-07T22:30:15"
    },
    {
      "step": "paso4_sync_faltantes",
      "status": "ok",
      "data": {
        "max_chats": 10,
        "consecutivos_para_detener": 30
      },
      "timestamp": "2026-01-07T22:35:42"
    }
  ]
}
```

**Estados posibles:**
- `ok`: Completado exitosamente
- `skipped`: Saltado (dry-run o flag)
- `error`: Error durante ejecución

---

## Parámetros de Configuración

### Argumentos CLI

```bash
python CLI/pipeline_cantidad_videos.py [opciones]
```

**Opciones:**
- `--max-chats N`: Limitar a N chats (default: sin límite)
- `--max-nuevos N`: Detener tras N consecutivos existentes (default: 30)
- `--dry-run`: Ejecutar sin modificar BD (solo lectura)
- `--skip-historico`: Saltar indexación histórica

### Constantes en Código

**`auditar_conteo_videos_chats.py`:**
- `UMBRAL_DIAS = 30`: Días de actividad reciente para re-escanear

**`sincronizar_faltantes_search.py`:**
- `consecutivos_para_detener`: Cuántos existentes seguidos antes de detener

**`indexador_historico.py`:**
- `LIMITE_MENSAJES = 10000`: Máximo de mensajes a procesar por chat
- `LIMITE_SIN_NUEVOS = 100`: Detener tras N consecutivos existentes

---

## Flujo de Datos Completo

```
┌─────────────────────────────────────────────────────────────┐
│ Paso 0: Telegram API → chats (activo=1, last_message_date) │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ Paso 1: Telegram API → chat_video_counts.videos_count      │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ Paso 2: video_messages → chat_video_counts.indexados       │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ Paso 3: video_messages → chat_video_counts.duplicados      │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ Paso 4: Telegram API → videos_telegram + video_messages    │
│         (sync recientes, detiene si X consecutivos existen) │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ Paso 5: Telegram API → videos_telegram + video_messages    │
│         (indexa histórico desde MIN(message_id) hacia atrás)│
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ Paso 6: video_messages → chat_video_counts.indexados (post)│
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ Paso 7: video_messages → chat_video_counts.duplicados(post)│
└─────────────────────────────────────────────────────────────┘
```

---

## Casos de Uso y Ejemplos

### Caso 1: Primer Escaneo de un Chat Nuevo

1. **Paso 0:** Chat detectado, marcado `activo=1`
2. **Paso 1:** API reporta 500 videos → `videos_count=500`
3. **Paso 2:** BD vacía → `indexados=0`
4. **Paso 3:** Sin duplicados → `duplicados=0`
5. **Paso 4:** Sincroniza videos recientes (ej. últimos 100)
6. **Paso 5:** Indexa histórico hasta completar o alcanzar límite
7. **Paso 6:** Recalcula → `indexados=500`
8. **Paso 7:** Detecta duplicados → `duplicados=5`

### Caso 2: Re-escaneo de Chat Activo

1. **Paso 0:** Chat ya existe, actualiza `last_message_date`
2. **Paso 1:** API reporta 520 videos (20 nuevos) → `videos_count=520`
3. **Paso 2:** BD tiene 500 → `indexados=500`
4. **Paso 4:** Sincroniza, encuentra 20 nuevos, detiene tras 30 consecutivos existentes
5. **Paso 6:** Recalcula → `indexados=520`

### Caso 3: Chat Abandonado

1. **Paso 0:** Chat no aparece en API → queda `activo=0`
2. **Pasos 1-7:** Chat ignorado (filtrado por `activo=1`)

---

## Troubleshooting

### Problema: `indexados` no coincide con `videos_count`

**Causa:** Sincronización incompleta o videos eliminados en Telegram.

**Solución:**
1. Ejecutar pipeline completo sin `--skip-historico`
2. Revisar logs de paso 4 y 5 para ver dónde se detuvo
3. Ajustar `--max-nuevos` si se detiene muy pronto

### Problema: Chats inactivos aparecen en resultados

**Causa:** Filtro `activo=1` faltante en query.

**Solución:** Verificar que todos los SELECT incluyan `JOIN chats c ON ... WHERE c.activo = 1`

### Problema: Logs no muestran `msg_id`

**Causa:** Versión antigua del código.

**Solución:** Verificar que logs incluyan formato:
```python
print(f"... | msg_id={m.id} | chat_id={chat_id} ...")
```

---

## Mantenimiento

### Ejecución Regular

**Recomendado:** Ejecutar pipeline cada 24-48 horas para mantener sincronización.

```bash
# Ejecución completa
python CLI/pipeline_cantidad_videos.py

# Solo sincronizar recientes (más rápido)
python CLI/pipeline_cantidad_videos.py --skip-historico

# Probar sin modificar BD
python CLI/pipeline_cantidad_videos.py --dry-run
```

### Limpieza de Datos

- Los duplicados NO se eliminan automáticamente, solo se cuentan
- Para limpiar duplicados manualmente, usar campo `oculto` en `videos_telegram`
- Valores de `oculto`: 0=visible, 1=oculto, 2=duplicado manual, 3=duplicado auto

---

## Referencias

### Archivos Principales

- `CLI/pipeline_cantidad_videos.py` - Orquestador principal
- `CLI/cantidad_videos/guardar_chats.py` - Paso 0
- `CLI/cantidad_videos/auditar_conteo_videos_chats.py` - Paso 1
- `CLI/cantidad_videos/recalcular_indexados_desde_bd.py` - Pasos 2 y 6
- `CLI/cantidad_videos/sincronizar_faltantes_search.py` - Paso 4
- `CLI/cantidad_videos/indexador_historico.py` - Paso 5
- `database/connection.py` - Schema y migraciones
- `database/chats.py` - Operaciones de BD para chats

### Dependencias

- `pyrogram` - Cliente de Telegram API
- `aiosqlite` - Base de datos SQLite async
- Python 3.10+

---

**Última actualización:** 2026-01-07
