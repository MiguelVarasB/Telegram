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

    function fillChannelMeta(data) {
        if (!dom.channelModalMeta) return;
        const parts = [];
        if (data.type) parts.push(`Tipo: ${data.type}`);
        if (data.username) parts.push(`Usuario: @${data.username}`);
        parts.push(`Indexados / Totales: ${(data.indexed_videos || 0)} / ${(data.total_videos || 0)}`);
        if (data.scanned_at) parts.push(`Último escaneo: ${data.scanned_at}`);
        if (data.last_message_date) parts.push(`Último mensaje: ${data.last_message_date}`);
        dom.channelModalMeta.innerHTML = parts.map(t => `<div>${helpers.escapeHtmlStats(t)}</div>`).join('');
    }

    async function fetchChannelInfo(id) {
        if (!id) return;
        if (dom.channelModalStatus) dom.channelModalStatus.textContent = 'Cargando info...';
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

    function openChannelModal(btn) {
        if (!btn || !dom.channelModalOverlay) return;
        state.channelModalData.chatId = btn.dataset.chatId || null;
        state.channelModalData.chatHref = btn.closest('.file-item')?.getAttribute('href') || null;

        if (dom.channelModalTitle) dom.channelModalTitle.textContent = btn.dataset.chatName || 'Canal';
        fillChannelMeta({
            type: btn.dataset.chatType,
            username: btn.dataset.username,
            indexed_videos: btn.dataset.indexed,
            total_videos: btn.dataset.total,
            scanned_at: btn.dataset.scannedAt,
            last_message_date: btn.dataset.lastMessage,
        });
        if (dom.channelModalStatus) dom.channelModalStatus.textContent = 'Cargando info...';
        dom.channelModalOverlay.classList.add('is-open');
        fetchChannelInfo(state.channelModalData.chatId);
    }

    function closeChannelModal() {
        if (!dom.channelModalOverlay) return;
        dom.channelModalOverlay.classList.remove('is-open');
        if (dom.channelModalStatus) dom.channelModalStatus.textContent = '';
    }

    function openChannelLink() {
        const chatId = state.channelModalData.chatId;
        const chatHref = state.channelModalData.chatHref;
        if (chatHref) window.location.href = chatHref;
        else if (chatId) window.location.href = `/channel/${encodeURIComponent(chatId)}`;
    }

    async function triggerChannelScan() {
        const chatId = state.channelModalData.chatId;
        if (!chatId || !dom.channelModalStatus) return;
        dom.channelModalStatus.textContent = 'Lanzando escaneo...';
        if (dom.channelModalScanBtn) dom.channelModalScanBtn.disabled = true;
        try {
            const res = await fetch(`/api/channel/${encodeURIComponent(chatId)}/scan`, { method: 'POST' });
            if (!res.ok) throw new Error();
            dom.channelModalStatus.textContent = 'Escaneo iniciado en segundo plano';
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
})();
