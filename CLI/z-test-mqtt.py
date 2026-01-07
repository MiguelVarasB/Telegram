import paho.mqtt.client as mqtt
import time

# Configuración basada en tu TECHNICAL_REFERENCE.md
MQTT_HOST = "127.0.0.2"  # Tu IP de proyecto verificada
MQTT_PORT = 1883
TOPIC = "bot/test/conexion"

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Conectado exitosamente al Broker en {MQTT_HOST}")
        client.subscribe(TOPIC)
    else:
        print(f"❌ Error de conexión. Código: {rc}")

def on_message(client, userdata, msg):
    print(f"📩 Mensaje recibido en el tópico [{msg.topic}]: {msg.payload.decode()}")
    print("🚀 ¡Prueba superada! MQTT está listo para tus 3 proyectos.")

# --- LA CORRECCIÓN ESTÁ AQUÍ ---
# Se añade 'CallbackAPIVersion.VERSION2' para compatibilidad con paho-mqtt 2.0+
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "Test_Xeon_PC")
# -------------------------------

client.on_connect = on_connect
client.on_message = on_message

print(f"Intentando conectar a {MQTT_HOST}...")
try:
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_start()

    mensaje = "Hola desde el Xeon de 16 núcleos"
    print(f"📤 Enviando mensaje: {mensaje}")
    client.publish(TOPIC, mensaje)

    time.sleep(2)
    client.loop_stop()
    client.disconnect()

except Exception as e:
    print(f"🔴 Error crítico: {e}")