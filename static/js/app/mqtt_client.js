/**
 * Cliente MQTT para el frontend usando Paho MQTT sobre WebSockets
 * Reemplaza las conexiones WebSocket nativas por MQTT
 */

class MQTTClientManager {
    constructor(config = {}) {
        this.broker = config.broker || '127.0.0.2';
        this.port = config.port || 9001; // Puerto WebSocket de Mosquitto
        this.clientId = config.clientId || `megatelegram_web_${Math.random().toString(16).substr(2, 8)}`;
        this.username = config.username || null;
        this.password = config.password || null;
        
        this.client = null;
        this.connected = false;
        this.subscriptions = new Map();
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 2000;
        
        console.log(`[MQTT] Inicializando cliente: ${this.clientId}`);
    }

    connect() {
        return new Promise((resolve, reject) => {
            try {
                // Crear cliente Paho MQTT
                this.client = new Paho.MQTT.Client(
                    this.broker,
                    this.port,
                    '/mqtt', // Path del WebSocket
                    this.clientId
                );

                // Configurar callbacks
                this.client.onConnectionLost = (responseObject) => {
                    this.onConnectionLost(responseObject);
                };

                this.client.onMessageArrived = (message) => {
                    this.onMessageArrived(message);
                };

                // Opciones de conexión
                const connectOptions = {
                    timeout: 10,
                    keepAliveInterval: 60,
                    cleanSession: true,
                    useSSL: false,
                    onSuccess: () => {
                        console.log('[MQTT] ✅ Conectado al broker');
                        this.connected = true;
                        this.reconnectAttempts = 0;
                        this.resubscribeAll();
                        resolve(true);
                    },
                    onFailure: (error) => {
                        console.error('[MQTT] ❌ Error de conexión:', error);
                        this.connected = false;
                        this.scheduleReconnect();
                        reject(error);
                    }
                };

                // Agregar credenciales si existen
                if (this.username && this.password) {
                    connectOptions.userName = this.username;
                    connectOptions.password = this.password;
                }

                // Conectar
                console.log(`[MQTT] Conectando a ${this.broker}:${this.port}...`);
                this.client.connect(connectOptions);

            } catch (error) {
                console.error('[MQTT] ❌ Error al crear cliente:', error);
                reject(error);
            }
        });
    }

    onConnectionLost(responseObject) {
        this.connected = false;
        if (responseObject.errorCode !== 0) {
            console.warn('[MQTT] ⚠️ Conexión perdida:', responseObject.errorMessage);
            this.scheduleReconnect();
        }
    }

    onMessageArrived(message) {
        const topic = message.destinationName;
        const payload = message.payloadString;
        
        console.log(`[MQTT] 📥 Mensaje recibido en '${topic}':`, payload.substring(0, 100));

        try {
            const data = JSON.parse(payload);
            
            // Buscar callbacks suscritos a este tópico
            for (const [pattern, callback] of this.subscriptions.entries()) {
                if (this.topicMatches(pattern, topic)) {
                    callback(data, topic);
                }
            }
        } catch (error) {
            console.error('[MQTT] ❌ Error procesando mensaje:', error);
        }
    }

    topicMatches(pattern, topic) {
        // Convertir patrón MQTT a regex
        // + coincide con un nivel, # coincide con múltiples niveles
        const regexPattern = pattern
            .replace(/\+/g, '[^/]+')
            .replace(/#/g, '.*')
            .replace(/\//g, '\\/');
        
        const regex = new RegExp(`^${regexPattern}$`);
        return regex.test(topic);
    }

    subscribe(topic, callback, qos = 0) {
        if (!this.client) {
            console.error('[MQTT] ❌ Cliente no inicializado');
            return false;
        }

        console.log(`[MQTT] 📡 Suscribiendo a '${topic}'`);
        
        // Guardar callback
        this.subscriptions.set(topic, callback);

        // Suscribir si está conectado
        if (this.connected) {
            try {
                this.client.subscribe(topic, {
                    qos: qos,
                    onSuccess: () => {
                        console.log(`[MQTT] ✅ Suscrito a '${topic}'`);
                    },
                    onFailure: (error) => {
                        console.error(`[MQTT] ❌ Error suscribiendo a '${topic}':`, error);
                    }
                });
                return true;
            } catch (error) {
                console.error(`[MQTT] ❌ Excepción suscribiendo a '${topic}':`, error);
                return false;
            }
        }

        return true;
    }

    unsubscribe(topic) {
        if (!this.client) {
            return false;
        }

        console.log(`[MQTT] 🔕 Desuscribiendo de '${topic}'`);
        
        this.subscriptions.delete(topic);

        if (this.connected) {
            try {
                this.client.unsubscribe(topic, {
                    onSuccess: () => {
                        console.log(`[MQTT] ✅ Desuscrito de '${topic}'`);
                    }
                });
            } catch (error) {
                console.error(`[MQTT] ❌ Error desuscribiendo de '${topic}':`, error);
            }
        }

        return true;
    }

    resubscribeAll() {
        console.log('[MQTT] 🔄 Re-suscribiendo a todos los tópicos...');
        for (const [topic] of this.subscriptions.entries()) {
            try {
                this.client.subscribe(topic, { qos: 0 });
            } catch (error) {
                console.error(`[MQTT] ❌ Error re-suscribiendo a '${topic}':`, error);
            }
        }
    }

    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error('[MQTT] ❌ Máximo de intentos de reconexión alcanzado');
            return;
        }

        this.reconnectAttempts++;
        const delay = this.reconnectDelay * this.reconnectAttempts;
        
        console.log(`[MQTT] 🔄 Reintentando conexión en ${delay}ms (intento ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        
        setTimeout(() => {
            this.connect().catch(err => {
                console.error('[MQTT] ❌ Fallo en reconexión:', err);
            });
        }, delay);
    }

    disconnect() {
        if (this.client && this.connected) {
            console.log('[MQTT] 🔌 Desconectando...');
            try {
                this.client.disconnect();
                this.connected = false;
            } catch (error) {
                console.error('[MQTT] ❌ Error desconectando:', error);
            }
        }
    }

    isConnected() {
        return this.connected;
    }
}

// Instancia global
let mqttClient = null;

// Función de inicialización
function initMQTTClient(config = {}) {
    if (mqttClient) {
        console.warn('[MQTT] Cliente ya inicializado');
        return mqttClient;
    }

    mqttClient = new MQTTClientManager(config);
    
    // Intentar conectar automáticamente
    mqttClient.connect().catch(err => {
        console.error('[MQTT] Error en conexión inicial:', err);
    });

    return mqttClient;
}

// Función para obtener el cliente
function getMQTTClient() {
    return mqttClient;
}

// Exportar para uso global
if (typeof window !== 'undefined') {
    window.MQTTClientManager = MQTTClientManager;
    window.initMQTTClient = initMQTTClient;
    window.getMQTTClient = getMQTTClient;
}
