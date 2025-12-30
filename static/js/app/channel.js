(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state || {};
    const helpers = App.helpers || {};
    const behaviors = App.behaviors = App.behaviors || {};

    state.channelModalData = state.channelModalData || {
        chatId: null,
        chatHref: null,
    };
    state.channelWS = state.channelWS || null;
    state.channelWSConnected = state.channelWSConnected || false;

    function ensureChannelWS() {
        if (state.channelWSConnected) return;
        const loc = window.location;
        const proto = (loc.protocol === 'https:') ? 'wss:' : 'ws:';
        const wsUrl = proto + '//' + loc.host + '/ws/folder/-1';
        const ws = new WebSocket(wsUrl);
        state.channelWS = ws;
        ws.onopen = () => { state.channelWSConnected = true; };
        ws.onclose = () => { state.channelWSConnected = false; state.channelWS = null; };
        ws.onerror = () => { try { ws.close(); } catch (e) {} };
        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.type === 'scan_done' && data.chat_id && state.channelModalData.chatId) {
                    if (String(data.chat_id) === String(state.channelModalData.chatId)) {
                        fillChannelMeta({
                            ...state.channelModalData,
                            indexed_videos: data.indexed_videos,
                            total_videos: data.total_videos,
                        });
                        state.channelModalData.indexed = data.indexed_videos;
                        state.channelModalData.total = data.total_videos;
                        if (dom.channelModalStatus) dom.channelModalStatus.textContent = 'Escaneo finalizado';
                    }
                }
            } catch (e) {
                // ignore malformed
            }
        };
    }

    // Rellena los datos descriptivos del canal en el modal (stats básicas + Telegram)
    function fillChannelMeta(data) {
        if (!dom.channelModalMeta) return;
        const indexed = Number(data.indexed_videos || 0);
        const total = Number(data.total_videos || indexed || 0);
        const duplicados = Number(data.duplicados || 0);
        const unicos = Math.max(total - duplicados, 0);
        const parts = [];
        if (data.type) parts.push(`Tipo: ${data.type}`);
        if (data.username) parts.push(`Usuario: @${data.username}`);
        if (data.username) {
            const safeUser = helpers.escapeHtmlStats(data.username);
            const tgLink = `https://t.me/${encodeURIComponent(data.username)}`;
            parts.push(`Enlace: <a href="${tgLink}" target="_blank" rel="noopener noreferrer">${tgLink}</a>`);
        } else if (data.chat_id) {
            parts.push(`ID Telegram: ${helpers.escapeHtmlStats(data.chat_id)}`);
        }
        parts.push(`Indexados / Totales / Únicos: ${indexed} / ${total} / ${unicos}`);
        if (duplicados) parts.push(`Duplicados: ${duplicados}`);
        if (data.scanned_at) parts.push(`Último escaneo: ${data.scanned_at}`);
        if (data.last_message_date) parts.push(`Último mensaje: ${data.last_message_date}`);
        if (data.members_count) parts.push(`Miembros: ${data.members_count}`);
        if (data.dc_id) parts.push(`DC: ${data.dc_id}`);
        if (data.is_verified !== undefined) parts.push(`Verificado: ${data.is_verified ? 'Sí' : 'No'}`);
        if (data.is_scam !== undefined) parts.push(`Marcado como scam: ${data.is_scam ? 'Sí' : 'No'}`);
        if (data.is_fake !== undefined) parts.push(`Marcado como fake: ${data.is_fake ? 'Sí' : 'No'}`);
        if (data.is_restricted !== undefined) parts.push(`Restringido: ${data.is_restricted ? 'Sí' : 'No'}`);
        if (data.restriction_reason) parts.push(`Motivo restricción: ${data.restriction_reason}`);
        if (data.description) parts.push(`Descripción: ${data.description}`);
        dom.channelModalMeta.innerHTML = parts.map(t => `<div>${helpers.escapeHtmlStats(t)}</div>`).join('');
    }

    // Trae info detallada del canal vía API y actualiza el modal
    async function fetchChannelInfo(id) {
        if (!id) return;
        if (dom.channelModalStatus) dom.channelModalStatus.textContent = 'Cargando metadatos de Telegram...';
        try {
            const res = await fetch(`/api/channel/${encodeURIComponent(id)}/info`);
            if (!res.ok) throw new Error();
            const data = await res.json();
            fillChannelMeta(data);
            if (dom.channelModalStatus) dom.channelModalStatus.textContent = '';
        } catch (e) {
            if (dom.channelModalStatus) dom.channelModalStatus.textContent = 'Error al cargar info del canal';
        }
    }

    // Abre el modal de canal usando data-* del botón origen
    function openChannelModal(btn) {
        if (!btn || !dom.channelModalOverlay) return;
        state.channelModalData.chatId = btn.dataset.chatId || null;
        state.channelModalData.chatHref = btn.closest('.file-item')?.getAttribute('href') || null;

        if (dom.channelModalTitle) dom.channelModalTitle.textContent = btn.dataset.chatName || 'Canal';
        const meta = {
            type: btn.dataset.chatType,
            username: btn.dataset.username,
            indexed_videos: btn.dataset.indexed,
            total_videos: btn.dataset.total,
            scanned_at: btn.dataset.scannedAt,
            last_message_date: btn.dataset.lastMessage,
        };
        fillChannelMeta(meta);
        state.channelModalData.chatType = meta.type;
        state.channelModalData.username = meta.username;
        state.channelModalData.indexed = meta.indexed_videos;
        state.channelModalData.total = meta.total_videos;
        state.channelModalData.lastMessage = meta.last_message_date;
        ensureChannelWS();
        if (dom.channelModalStatus) dom.channelModalStatus.textContent = '';
        dom.channelModalOverlay.classList.add('is-open');
        fetchChannelInfo(state.channelModalData.chatId);
    }

    // Cierra el modal de canal y limpia estado de status
    function closeChannelModal() {
        if (!dom.channelModalOverlay) return;
        dom.channelModalOverlay.classList.remove('is-open');
        if (dom.channelModalStatus) dom.channelModalStatus.textContent = '';
    }

    // Abre el enlace del canal (o fallback a /channel/:id)
    function openChannelLink() {
        const chatId = state.channelModalData.chatId;
        const chatHref = state.channelModalData.chatHref;
        if (chatHref) window.location.href = chatHref;
        else if (chatId) window.location.href = `/channel/${encodeURIComponent(chatId)}`;
    }

    // Lanza POST para reindexar el canal y muestra estado
    async function triggerChannelScan() {
        const chatId = state.channelModalData.chatId;
        if (!chatId || !dom.channelModalStatus) return;
        dom.channelModalStatus.textContent = 'Lanzando escaneo...';
        if (dom.channelModalScanBtn) dom.channelModalScanBtn.disabled = true;
        try {
            const res = await fetch(`/api/channel/${encodeURIComponent(chatId)}/scan`, { method: 'POST' });
            if (!res.ok) throw new Error();
            dom.channelModalStatus.textContent = 'Escaneo iniciado (esperando señal)...';
            // Fallback opcional: refrescar una vez si en 12s no llegó el evento
            setTimeout(async () => {
                if (dom.channelModalStatus && dom.channelModalStatus.textContent?.includes('esperando señal')) {
                    try {
                        const infoRes = await fetch(`/api/channel/${encodeURIComponent(chatId)}/info`);
                        if (infoRes.ok) {
                            const data = await infoRes.json();
                            fillChannelMeta(data);
                            state.channelModalData.indexed = data.indexed_videos;
                            state.channelModalData.total = data.total_videos;
                        }
                    } catch (e) {}
                    dom.channelModalStatus.textContent = 'Escaneo finalizado';
                }
            }, 12000);
        } catch (e) {
            dom.channelModalStatus.textContent = 'Error al iniciar escaneo';
        } finally {
            if (dom.channelModalScanBtn) dom.channelModalScanBtn.disabled = false;
        }
    }

    // Exponer para inline handler de la card (compatibilidad)
    window.openChannelModalFromButton = function (btn) {
        openChannelModal(btn);
        return false;
    };

    behaviors.openChannelModal = openChannelModal;
    behaviors.closeChannelModal = closeChannelModal;
    behaviors.openChannelLink = openChannelLink;
    behaviors.triggerChannelScan = triggerChannelScan;
    behaviors.fillChannelMeta = fillChannelMeta;
})();
