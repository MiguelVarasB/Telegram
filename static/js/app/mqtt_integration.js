/**
 * Integración MQTT para reemplazar WebSockets en la aplicación
 * Mantiene compatibilidad con la lógica existente
 */

(function() {
    const App = window.App = window.App || {};
    
    // Configuración MQTT
    const MQTT_CONFIG = {
        broker: '127.0.0.2',
        port: 9001, // Puerto WebSocket de Mosquitto (configurar en mosquitto.conf)
        clientId: null, // Se genera automáticamente
        username: null,
        password: null
    };

    /**
     * Inicializa el cliente MQTT y configura suscripciones
     */
    function initMQTT() {
        console.log('[MQTT Integration] Inicializando...');
        
        // Inicializar cliente MQTT
        const client = window.initMQTTClient(MQTT_CONFIG);
        
        if (!client) {
            console.error('[MQTT Integration] No se pudo inicializar el cliente MQTT');
            return;
        }

        // Esperar a que se conecte
        const checkConnection = setInterval(() => {
            if (client.isConnected()) {
                clearInterval(checkConnection);
                console.log('[MQTT Integration] Cliente conectado, configurando suscripciones...');
                setupSubscriptions(client);
            }
        }, 500);

        // Timeout de 10 segundos
        setTimeout(() => {
            clearInterval(checkConnection);
            if (!client.isConnected()) {
                console.warn('[MQTT Integration] Timeout esperando conexión MQTT');
            }
        }, 10000);

        // Guardar referencia global
        App.mqttClient = client;
    }

    /**
     * Configura las suscripciones MQTT según el contexto de la página
     */
    function setupSubscriptions(client) {
        const container = document.querySelector('.container[data-folder-id]');
        
        // Suscripción a carpetas
        if (container) {
            const folderId = container.getAttribute('data-folder-id');
            if (folderId) {
                subscribeFolderUpdates(client, folderId);
            }
        }

        // Suscripción a progreso de descargas (global)
        subscribeDownloadProgress(client);

        // Suscripción a cambios de visibilidad de videos
        subscribeVideoVisibility(client);
    }

    /**
     * Suscribe a actualizaciones de carpeta
     */
    function subscribeFolderUpdates(client, folderId) {
        const topic = `bot/folder/${folderId}/scan`;
        
        console.log(`[MQTT Integration] Suscribiendo a carpeta ${folderId}`);
        
        client.subscribe(topic, (payload, receivedTopic) => {
            handleFolderMessage(payload, folderId);
        }, 1);

        // También suscribirse al tópico de refresh
        const refreshTopic = `bot/folder/${folderId}/refresh`;
        client.subscribe(refreshTopic, (payload) => {
            console.log('[MQTT Integration] Refresh recibido para carpeta', folderId);
            if (payload.type === 'refresh') {
                // Recargar items de la carpeta
                if (typeof window.App?.behaviors?.reloadFolderItems === 'function') {
                    window.App.behaviors.reloadFolderItems();
                }
            }
        }, 0);
    }

    /**
     * Maneja mensajes de carpeta (escaneo batch)
     */
    function handleFolderMessage(payload, folderId) {
        const state = App.state || {};
        state.batchScan = state.batchScan || { jobId: null, total: 0, done: 0, running: 0 };

        const btnSyncFaltantes = document.getElementById('btn-sync-faltantes');

        function updateBatchButton(text, disabled) {
            if (!btnSyncFaltantes) return;
            btnSyncFaltantes.textContent = text;
            btnSyncFaltantes.disabled = !!disabled;
        }

        function formatBatchLabel() {
            if (!state.batchScan || !state.batchScan.jobId) return 'Sincronizar faltantes';
            const { done, total, running } = state.batchScan;
            return `Escaneando (${done}/${total}, ${running} activos)`;
        }

        // Procesar según tipo de mensaje
        switch (payload.type) {
            case 'batch_scan_start':
                state.batchScan = {
                    jobId: payload.job_id,
                    total: payload.total || 0,
                    done: 0,
                    running: 0,
                };
                updateBatchButton(formatBatchLabel(), true);
                console.log('[MQTT Integration] Escaneo batch iniciado:', payload);
                break;

            case 'batch_scan_update':
                if (state.batchScan.jobId === payload.job_id) {
                    state.batchScan.done = payload.done ?? state.batchScan.done;
                    state.batchScan.running = payload.running ?? state.batchScan.running;
                    updateBatchButton(formatBatchLabel(), true);
                }
                break;

            case 'batch_scan_done':
                if (state.batchScan.jobId === payload.job_id) {
                    state.batchScan.done = payload.done ?? state.batchScan.done;
                    state.batchScan.running = 0;
                    updateBatchButton('Sincronizar faltantes', false);
                    console.log('[MQTT Integration] Escaneo batch completado');
                }
                break;

            case 'scan_done':
                if (payload.chat_id) {
                    // Si el modal actual corresponde al chat, actualiza
                    if (window.App?.state?.channelModalData?.chatId == payload.chat_id) {
                        if (typeof window.App?.behaviors?.fillChannelMeta === 'function') {
                            window.App.behaviors.fillChannelMeta(payload.chat_id);
                        }
                    }
                }
                break;
        }
    }

    /**
     * Suscribe a progreso de descargas
     */
    function subscribeDownloadProgress(client) {
        const topic = 'bot/video/download/progress';
        
        console.log('[MQTT Integration] Suscribiendo a progreso de descargas');
        
        client.subscribe(topic, (payload) => {
            handleDownloadProgress(payload);
        }, 1);
    }

    /**
     * Maneja mensajes de progreso de descarga
     */
    function handleDownloadProgress(payload) {
        console.log('[MQTT Integration] Progreso de descarga:', payload);

        // Actualizar UI si existe el modal de descargas
        if (typeof window.App?.behaviors?.updateDownloadProgress === 'function') {
            window.App.behaviors.updateDownloadProgress(payload);
        }

        // Emitir evento personalizado para otros componentes
        const event = new CustomEvent('mqtt:download:progress', {
            detail: payload
        });
        document.dispatchEvent(event);
    }

    /**
     * Suscribe a cambios de visibilidad de videos
     */
    function subscribeVideoVisibility(client) {
        const topic = 'bot/video/status/visibility';
        
        console.log('[MQTT Integration] Suscribiendo a cambios de visibilidad');
        
        client.subscribe(topic, (payload) => {
            handleVideoVisibility(payload);
        }, 1);
    }

    /**
     * Maneja mensajes de cambio de visibilidad
     */
    function handleVideoVisibility(payload) {
        console.log('[MQTT Integration] Cambio de visibilidad:', payload);

        // Actualizar UI si es necesario
        const videoElement = document.querySelector(`[data-video-id="${payload.video_id}"]`);
        if (videoElement) {
            videoElement.dataset.hidden = payload.oculto;
            
            if (payload.oculto > 0) {
                videoElement.classList.add('is-hidden-video');
            } else {
                videoElement.classList.remove('is-hidden-video');
            }
        }

        // Emitir evento personalizado
        const event = new CustomEvent('mqtt:video:visibility', {
            detail: payload
        });
        document.dispatchEvent(event);
    }

    /**
     * Publica un mensaje MQTT (para futuras extensiones)
     */
    function publishMQTT(topic, payload) {
        const client = App.mqttClient;
        if (!client || !client.isConnected()) {
            console.warn('[MQTT Integration] Cliente no conectado, no se puede publicar');
            return false;
        }

        try {
            const message = new Paho.MQTT.Message(JSON.stringify(payload));
            message.destinationName = topic;
            message.qos = 1;
            client.client.send(message);
            console.log(`[MQTT Integration] Mensaje publicado en '${topic}'`);
            return true;
        } catch (error) {
            console.error('[MQTT Integration] Error publicando mensaje:', error);
            return false;
        }
    }

    // Exportar funciones
    App.mqtt = {
        init: initMQTT,
        publish: publishMQTT,
        getClient: () => App.mqttClient
    };

    // Auto-inicializar cuando el DOM esté listo
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initMQTT);
    } else {
        // DOM ya está listo
        initMQTT();
    }

    console.log('[MQTT Integration] Módulo cargado');
})();
