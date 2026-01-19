(function () {
    const App = window.App = window.App || {};
    const state = App.state = App.state || {};
    const behaviors = App.behaviors = App.behaviors || {};

    state.batchScan = state.batchScan || { jobId: null, total: 0, done: 0, running: 0 };

    // --- Conexión Principal ---
    behaviors.setupFolderConnection = function() {
        try {
            const container = document.querySelector('.container[data-folder-id]');
            if (!container) return;
            const folderId = container.getAttribute('data-folder-id');
            if (!folderId) return;

            // Priorizar MQTT sobre WebSocket si existe cliente conectado
            if (window.App && window.App.mqttClient && window.App.mqttClient.isConnected()) {
                console.log(`[Network] Usando MQTT para carpeta ${folderId}`);
                setupFolderMQTT(folderId);
            } else {
                console.log(`[Network] Usando WebSocket para carpeta ${folderId}`);
                setupFolderWS(folderId);
            }
            
            // Carga inicial (fallback por si WS falla)
            reloadFolderItems(folderId);
        } catch (e) {
            console.warn('[Network] Error setupFolderConnection', e);
        }
    };

    function setupFolderMQTT(folderId) {
        const client = window.App.mqttClient;
        if (!client) return;

        client.subscribe(`bot/folder/${folderId}/scan`, (payload) => handleFolderMessage(payload), 1);
        client.subscribe(`bot/folder/${folderId}/refresh`, (payload) => {
            if (payload.type === 'refresh') reloadFolderItems(folderId);
        }, 0);
    }

    function setupFolderWS(folderId) {
        if (folderId !== '-1') return; // Carpetas normales cargan por API directo

        const loc = window.location;
        const proto = (loc.protocol === 'https:') ? 'wss:' : 'ws:';
        
        // Parámetros de URL (limite)
        const limitInput = document.getElementById('search-limit');
        const urlParams = new URLSearchParams(loc.search);
        const limitVal = (limitInput && limitInput.value) || urlParams.get('limite') || '';
        const qs = limitVal ? `?limite=${encodeURIComponent(limitVal)}` : '';

        const wsUrl = `${proto}//${loc.host}/ws/folder/${encodeURIComponent(folderId)}${qs}`;
        
        let ws;
        try { ws = new WebSocket(wsUrl); } catch (e) { return; }

        ws.onmessage = (event) => {
            try {
                const payload = JSON.parse(event.data);
                handleFolderMessage(payload);
            } catch (e) {}
        };
        
        window.addEventListener('beforeunload', () => { if(ws.readyState===1) ws.close(); });
    }

    function reloadFolderItems(folderId) {
        fetch('/api/folder/' + encodeURIComponent(folderId))
            .then(res => res.ok ? res.json() : [])
            .then(data => {
                if (typeof window.renderItems === 'function') window.renderItems(data);
            })
            .catch(console.error);
    }

    function handleFolderMessage(payload) {
        if (!payload || !payload.type) return;

        if (payload.type === 'init' || payload.type === 'refresh') {
            if (Array.isArray(payload.items) && typeof window.renderItems === 'function') {
                window.renderItems(payload.items);
            }
        }
        
        // Actualizar estado de escaneo (Botón Sincronizar)
        if (payload.type.startsWith('batch_scan')) {
            if (payload.type === 'batch_scan_start') {
                state.batchScan = { jobId: payload.job_id, total: payload.total || 0, done: 0, running: 0 };
            } else if (payload.type === 'batch_scan_update' && state.batchScan.jobId === payload.job_id) {
                state.batchScan.done = payload.done ?? state.batchScan.done;
                state.batchScan.running = payload.running ?? state.batchScan.running;
            } else if (payload.type === 'batch_scan_done' && state.batchScan.jobId === payload.job_id) {
                state.batchScan.done = payload.done ?? state.batchScan.done;
                state.batchScan.running = 0;
            }
            // Actualizar UI del botón (si existe la función)
            if (behaviors.updateBatchButtonUI) behaviors.updateBatchButtonUI();
        }

        // Actualizar modal de canal si está abierto
        if (payload.type === 'scan_done' && payload.chat_id) {
            updateChannelModal(payload);
        }
    }

    function updateChannelModal(payload) {
        const modalData = window.App?.state?.channelModalData;
        if (modalData && modalData.chatId == payload.chat_id) {
            if (typeof behaviors.fillChannelMeta === 'function') {
                behaviors.fillChannelMeta({
                    type: modalData.chatType,
                    username: modalData.username,
                    indexed_videos: payload.indexed_videos,
                    total_videos: payload.total_videos,
                    scanned_at: new Date().toISOString(),
                    last_message_date: modalData.lastMessage,
                });
            }
            const statusEl = document.getElementById('channel-modal-status');
            if (statusEl) statusEl.textContent = 'Escaneo finalizado';
        }
    }
})();