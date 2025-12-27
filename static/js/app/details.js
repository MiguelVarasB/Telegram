(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state || {};
    const helpers = App.helpers || {};
    const behaviors = App.behaviors = App.behaviors || {};

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
    }

    function clearVideoDetails() {
        if (dom.detailsTitle) dom.detailsTitle.textContent = '';
        if (dom.detailsMetaLine) dom.detailsMetaLine.textContent = '';
        if (dom.detailsExtraLine) dom.detailsExtraLine.textContent = '';
        if (dom.detailsCaption) dom.detailsCaption.textContent = '';
        if (dom.detailsIds) dom.detailsIds.textContent = '';
    }

    function clearVideoMessages() {
        if (dom.messagesContainer) {
            dom.messagesContainer.innerHTML = '';
        }
    }

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
})();
