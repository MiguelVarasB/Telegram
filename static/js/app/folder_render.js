(function () {
    const App = window.App = window.App || {};
    const behaviors = App.behaviors = App.behaviors || {};

    // Escapado seguro de HTML
    function escapeHtml(str) {
        if (!str) return '';
        return String(str).replace(/[&<>"']/g, function(m) {
            switch (m) {
                case '&': return '&amp;';
                case '<': return '&lt;';
                case '>': return '&gt;';
                case '"': return '&quot;';
                case "'": return '&#39;';
            }
        });
    }

    // Renderizado por lotes (Batch Rendering)
    function renderItems(items) {
        const container = document.querySelector('.container[data-folder-id]');
        if (!container) return;
        const grid = container.querySelector('.grid-view');
        if (!grid) return;

        if (!Array.isArray(items)) return;
        
        // 1. Limpiar una sola vez
        grid.innerHTML = '';
        
        const BATCH_SIZE = 50;
        let currentIndex = 0;

        function processBatch() {
            const batch = items.slice(currentIndex, currentIndex + BATCH_SIZE);
            
            if (batch.length === 0) {
                finishRendering(items, grid);
                return;
            }

            let html = '';
            batch.forEach(function (item) {
                // Variables seguras
                const i_folderId = item.folder_id || '';
                const chatId = item.chat_id || '';
                const faltantes = item.faltantes || 0;
                const indexed = item.indexed_videos || '0';
                const total = item.total_videos || '0';
                const scannedAt = item.scanned_at || '';
                const chatType = item.chat_type || '';
                const lastMsg = item.last_message_date || '';
                const username = item.username || '';
                const name = escapeHtml(item.name || '');
                const count = escapeHtml(item.count || '');
                const link = item.link || '#';
                const type = item.type || 'chat';
                const photoId = item.photo_id;
                const telegramLink = item.telegram_link || '';

                if (type === 'video') {
                    const videoId = item.video_id || '';
                    const streamUrl = item.stream_url || (item.link || '').replace('/play/', '/video_stream/');
                    html += `<div class="file-item" data-item-type="video" data-stream-url="${escapeHtml(streamUrl)}"` +
                        (videoId ? ` data-video-id="${escapeHtml(videoId)}"` : '') +
                        (chatId ? ` data-chat-id="${escapeHtml(chatId)}"` : '') +
                        `><a href="${link}" class="video-thumb-link" aria-label="Abrir video"><div class="icon-box video-box">` +
                        `<div class="video-hidden-indicator"><i class="fas fa-eye-slash"></i></div>` +
                        `<div class="video-loading-spinner"><i class="fas fa-circle-notch fa-spin"></i></div>`;
                    if (photoId) {
                        html += `<img src="/api/photo/${encodeURIComponent(photoId)}?chat_id=${encodeURIComponent(chatId)}&video_id=${encodeURIComponent(videoId)}" class="video-thumb" loading="lazy">`;
                    } else {
                        html += `<i class="fas fa-file-video file-video"></i>`;
                    }
                    if (item.duration_text) {
                        html += `<div class="video-duration">${escapeHtml(item.duration_text)}</div>`;
                    }
                    html += `</div></a><div class="file-name" title="${name}">${name}</div><div class="file-info">${count}</div></div>`;
                } else {
                    // Renderizado de Chat
                    html += `<a href="${link}" class="file-item" data-item-type="${escapeHtml(type)}" data-chat-id="${escapeHtml(chatId)}" data-faltantes="${escapeHtml(faltantes)}" data-indexed="${escapeHtml(indexed)}" data-total="${escapeHtml(total)}" data-scanned-at="${escapeHtml(scannedAt)}">`;
                    if (telegramLink) {
                        html += `<span class="btn-open-telegram" data-telegram-link="${escapeHtml(telegramLink)}" title="Abrir en Telegram"><i class="fa-brands fa-telegram"></i></span>`;
                    }
                    html += `<div class="icon-box folder-box"><i class="fas fa-folder folder-chat folder-icon"></i>`;
                    if (item.photo_url) {
                        html += `<img src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==" data-src="${escapeHtml(item.photo_url)}" class="profile-img folder-overlay lazy-chat-photo" loading="lazy">`;
                    }
                    html += `</div><div class="file-name" title="${name}">${name}</div><div class="file-info">${count}</div>`;
                    
                    if (i_folderId) {
                        html += `<button type="button" class="btn-sync open-channel-modal" onclick="return window.openChannelModalFromButton && window.openChannelModalFromButton(this);" data-chat-id="${escapeHtml(chatId)}" data-chat-name="${name}" data-indexed="${escapeHtml(indexed)}" data-total="${escapeHtml(total)}" data-scanned-at="${escapeHtml(scannedAt)}" data-chat-type="${escapeHtml(chatType)}" data-last-message="${escapeHtml(lastMsg)}" data-username="${escapeHtml(username)}" data-telegram-link="${escapeHtml(telegramLink)}">Info / Indexar</button>`;
                    }
                    html += `</a>`;
                }
            });

            grid.insertAdjacentHTML('beforeend', html);
            currentIndex += BATCH_SIZE;
            requestAnimationFrame(processBatch);
        }

        processBatch();
    }

    function finishRendering(items, grid) {
        // Re-activar comportamientos globales tras renderizar
        if (items.some((it) => it.folder_id)) {
            const btns = grid.querySelectorAll('.btn-sync.open-channel-modal');
            btns.forEach((btn) => btn.style.display = '');
        }
        
        // Mostrar botón de sync si estamos en carpeta "Todos" (-1)
        const btnSync = document.getElementById('btn-sync-faltantes');
        if (btnSync && window.location.pathname.includes('/folder/-1')) {
            btnSync.style.display = '';
        }

        if (window.refreshLazyChatPhotos) window.refreshLazyChatPhotos();
        if (behaviors.applyHiddenStateToAll) behaviors.applyHiddenStateToAll();
        if (behaviors.applyDuplicateFilter) behaviors.applyDuplicateFilter();
        if (behaviors.applyWatchLaterStateToAll) behaviors.applyWatchLaterStateToAll();
        if (behaviors.setupLazyChatPhotos) behaviors.setupLazyChatPhotos();
        if (behaviors.setupHoverPreviews) behaviors.setupHoverPreviews();
    }

    // Exponer globalmente
    window.renderItems = renderItems;
    behaviors.renderItems = renderItems;
})();