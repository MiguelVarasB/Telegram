(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state || {};
    const helpers = App.helpers || {};
    const behaviors = App.behaviors = App.behaviors || {};

    // Rellena panel de detalles usando los data-* del elemento de video
    function fillVideoDetailsFromElement(el) {
        if (!dom.detailsContainer || !el) return;

        const name = el.dataset.name || '';
        const size = el.dataset.size || '';
        const duration = el.dataset.duration || '';
        const width = el.dataset.width || '';
        const height = el.dataset.height || '';
        const mime = el.dataset.mime || '';
        const date = el.dataset.date || '';
        const views = el.dataset.views || '';
        const caption = el.dataset.caption || '';
        const chatId = el.dataset.chatId || '';
        const messageId = el.dataset.messageId || '';
        const fileUniqueId = el.dataset.fileUniqueId || '';
        const messagesCount = el.dataset.messagesCount || '';
        const watchLaterFlag = el.dataset.watchLater === '1';

        if (dom.detailsTitle) dom.detailsTitle.textContent = name;

        const metaParts = [];
        if (size) metaParts.push(size);
        if (duration) metaParts.push('Duración: ' + duration);
        if (width && height) metaParts.push('Resolución: ' + width + 'x' + height);
        if (mime) metaParts.push(mime);
        if (dom.detailsMetaLine) dom.detailsMetaLine.textContent = metaParts.join(' · ');

        const extraParts = [];
        if (date) extraParts.push('Fecha: ' + date);
        if (views) extraParts.push('Vistas: ' + helpers.formatThousands(views));
        if (dom.detailsExtraLine) dom.detailsExtraLine.textContent = extraParts.join(' · ');

        if (dom.detailsCaption) dom.detailsCaption.textContent = caption;

        const idParts = [];
        if (chatId) idParts.push('Chat ID: ' + chatId);
        if (messageId) idParts.push('Mensaje ID: ' + messageId);
        if (fileUniqueId) idParts.push('File Unique ID: ' + fileUniqueId);
        if (messagesCount) idParts.push('Mensajes asociados: ' + helpers.formatThousands(messagesCount));
        if (dom.detailsIds) dom.detailsIds.textContent = idParts.join(' · ');

        if (dom.detailsWatchLaterPill) {
            dom.detailsWatchLaterPill.style.display = watchLaterFlag ? 'block' : 'none';
        }

        // Prefill formulario de edición
        if (dom.videoEditTitleInput) dom.videoEditTitleInput.value = name;
        if (dom.videoEditDurationInput) dom.videoEditDurationInput.value = duration;
    }

    // Limpia textos del panel de detalles
    function clearVideoDetails() {
        if (dom.detailsTitle) dom.detailsTitle.textContent = '';
        if (dom.detailsMetaLine) dom.detailsMetaLine.textContent = '';
        if (dom.detailsExtraLine) dom.detailsExtraLine.textContent = '';
        if (dom.detailsCaption) dom.detailsCaption.textContent = '';
        if (dom.detailsIds) dom.detailsIds.textContent = '';
    }

    // Limpia contenedor de mensajes relacionados
    function clearVideoMessages() {
        if (dom.messagesContainer) {
            dom.messagesContainer.innerHTML = '';
        }
    }

    async function saveVideoMetadata() {
        if (!state.currentVideoId || !dom.videoEditTitleInput || !dom.videoEditDurationInput) return;
        const statusEl = dom.videoEditStatus;
        const title = dom.videoEditTitleInput.value || '';
        const duration = dom.videoEditDurationInput.value || '';

        const setStatus = (msg, ok = false) => {
            if (statusEl) {
                statusEl.textContent = msg || '';
                statusEl.style.color = ok ? '#7bd88f' : 'var(--text-muted)';
            }
        };

        setStatus('Guardando...');
        try {
            const res = await fetch(`/api/video/${encodeURIComponent(state.currentVideoId)}/metadata`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title, duration }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                throw new Error(err?.detail || 'Error al guardar');
            }
            const data = await res.json();

            // Actualizar dataset y UI de la tarjeta actual
            const el = state.currentVideoElement;
            if (el) {
                el.dataset.name = data.title || '';
                el.dataset.duration = data.duration_text || '';
                const nameNode = el.querySelector('.file-name');
                if (nameNode) nameNode.textContent = data.title || '';
                const durationNode = el.querySelector('.video-duration');
                if (durationNode) {
                    durationNode.textContent = data.duration_text || '';
                    durationNode.style.display = data.duration_text ? 'block' : 'none';
                }
            }

            // Refrescar panel de detalles
            behaviors.fillVideoDetailsFromElement?.(el);

            setStatus('Cambios guardados', true);
        } catch (e) {
            console.error('saveVideoMetadata error', e);
            setStatus(e.message || 'Error al guardar');
        }
    }

    // Carga mensajes relacionados a un video vía API y los renderiza
    async function loadVideoMessages(videoId) {
        if (!dom.messagesContainer) return;
        clearVideoMessages();
        if (!videoId) return;

        dom.messagesContainer.textContent = 'Cargando mensajes...';
        try {
            const res = await fetch('/api/video/' + encodeURIComponent(videoId) + '/messages');
            if (!res.ok) {
                dom.messagesContainer.textContent = 'Error al cargar mensajes';
                return;
            }
            const data = await res.json();
            const msgs = data.messages || [];
            if (!msgs.length) {
                dom.messagesContainer.textContent = 'No hay otros mensajes registrados para este video.';
                return;
            }
            let html = '';
            msgs.forEach(function (m) {
                const line1Parts = [];
                if (m.date) line1Parts.push(m.date);
                line1Parts.push('chat ' + m.chat_id + ' · msg ' + m.message_id);

                const line2Parts = [];
                if (m.views !== null && m.views !== undefined) line2Parts.push('Vistas: ' + helpers.formatThousands(m.views));
                if (m.forwards !== null && m.forwards !== undefined) line2Parts.push('Reenvíos: ' + helpers.formatThousands(m.forwards));
                if (m.forward_from_chat_title || m.forward_from_chat_id) {
                    const ffTitle = helpers.escapeHtmlStats(m.forward_from_chat_title || '');
                    const ffId = (m.forward_from_chat_id !== null && m.forward_from_chat_id !== undefined)
                        ? (' (' + helpers.escapeHtmlStats(m.forward_from_chat_id) + ')')
                        : '';
                    line2Parts.push('Forward de: ' + ffTitle + ffId);
                }

                const caption = m.caption || '';

                html += '<div style="margin-bottom:8px; padding:6px 8px; background:#202020; border-radius:4px;">'
                    + '<div style="font-size:0.75rem; color:#ccc;">' + line1Parts.join(' · ') + '</div>'
                    + '<div style="font-size:0.75rem; color:#aaa;">' + line2Parts.join(' · ') + '</div>'
                    + (caption ? '<div style="margin-top:4px; white-space:pre-wrap;">' + caption.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</div>' : '')
                    + '</div>';
            });
            dom.messagesContainer.innerHTML = html;
        } catch (e) {
            console.error('Error cargando mensajes de video', e);
            dom.messagesContainer.textContent = 'Error al cargar mensajes';
        }
    }

    behaviors.fillVideoDetailsFromElement = fillVideoDetailsFromElement;
    behaviors.clearVideoDetails = clearVideoDetails;
    behaviors.clearVideoMessages = clearVideoMessages;
    behaviors.loadVideoMessages = loadVideoMessages;
    behaviors.saveVideoMetadata = saveVideoMetadata;
})();
