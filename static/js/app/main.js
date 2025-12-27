(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state || {};
    const behaviors = App.behaviors || {};

    // --- Event Listeners Globales ---
    function bindGlobalEvents() {
        if (dom.modalOverlay) {
            dom.modalOverlay.addEventListener('click', function (ev) {
                if (ev.target === dom.modalOverlay) behaviors.closeVideoModal?.();
            });
        }
        if (dom.modalClose) dom.modalClose.addEventListener('click', behaviors.closeVideoModal);

        if (dom.statsButton) dom.statsButton.addEventListener('click', behaviors.openStatsModal);
        if (dom.statsOverlay) {
            dom.statsOverlay.addEventListener('click', function (ev) {
                if (ev.target === dom.statsOverlay) behaviors.closeStatsModal?.();
            });
        }
        if (dom.statsClose) dom.statsClose.addEventListener('click', behaviors.closeStatsModal);

        if (dom.hideVideoButton) {
            dom.hideVideoButton.addEventListener('click', function () {
                if (!state.currentVideoId || !state.currentVideoElement || !state.hiddenVideos) return;
                const isHidden = state.hiddenVideos.has(state.currentVideoId);
                if (isHidden) {
                    state.hiddenVideos.delete(state.currentVideoId);
                    state.currentVideoElement.dataset.hidden = '0';
                    state.currentVideoElement.classList.remove('is-hidden-video');
                } else {
                    state.hiddenVideos.add(state.currentVideoId);
                    state.currentVideoElement.dataset.hidden = '1';
                    state.currentVideoElement.classList.add('is-hidden-video');
                }
                behaviors.saveHiddenVideos?.();
                behaviors.updateHideVideoButton?.();
                behaviors.applyDuplicateFilter?.();
            });
        }

        if (dom.msgToggleBtn && dom.messagesContainer && dom.msgToggleIcon) {
            dom.msgToggleBtn.addEventListener('click', function() {
                const isHidden = (dom.messagesContainer.style.display === 'none');
                if (isHidden) {
                    dom.messagesContainer.style.display = 'block';
                    dom.msgToggleIcon.classList.remove('fa-chevron-down');
                    dom.msgToggleIcon.classList.add('fa-chevron-up');
                } else {
                    dom.messagesContainer.style.display = 'none';
                    dom.msgToggleIcon.classList.remove('fa-chevron-up');
                    dom.msgToggleIcon.classList.add('fa-chevron-down');
                }
            });
        }

        if (dom.toggleDuplicates) {
            dom.toggleDuplicates.addEventListener('change', behaviors.applyDuplicateFilter);
        }

        // Delegación click para videos
        document.addEventListener('click', function (event) {
            const target = event.target.closest('.file-item[data-item-type="video"]');
            if (!target) return;
            const streamUrl = target.dataset.streamUrl || target.dataset.streamUrl;
            if (!streamUrl) return;
            event.preventDefault();
            behaviors.openVideoModal?.(streamUrl, target);
        });

        // Botón "Info / Indexar" en cards de chat: abrir modal de canal
        document.addEventListener('click', function (event) {
            const btn = event.target.closest('.open-channel-modal');
            if (!btn) return;
            event.preventDefault();
            event.stopPropagation();
            behaviors.openChannelModal?.(btn);
        });

        // Cerrar modal de canal
        if (dom.channelModalClose) {
            dom.channelModalClose.addEventListener('click', behaviors.closeChannelModal);
        }
        if (dom.channelModalOverlay) {
            dom.channelModalOverlay.addEventListener('click', function (ev) {
                if (ev.target === dom.channelModalOverlay) behaviors.closeChannelModal?.();
            });
        }
        // Botón abrir canal
        if (dom.channelModalOpenBtn) {
            dom.channelModalOpenBtn.addEventListener('click', behaviors.openChannelLink);
        }
        // Botón indexar faltantes
        if (dom.channelModalScanBtn) {
            dom.channelModalScanBtn.addEventListener('click', behaviors.triggerChannelScan);
        }

        // Delegación click para telegram
        document.addEventListener('click', function (event) {
            const tgBtn = event.target.closest('.btn-open-telegram');
            if (!tgBtn) return;
            const tgLink = tgBtn.getAttribute('data-telegram-link') || '';
            event.preventDefault();
            event.stopPropagation();
            window.open(tgLink, '_blank', 'noopener,noreferrer');
        });

        // Tecla Escape
        document.addEventListener('keydown', function (event) {
            if (event.key !== 'Escape') return;
            if (dom.statsOverlay && dom.statsOverlay.classList.contains('is-open')) {
                behaviors.closeStatsModal?.();
                return;
            }
            if (dom.modalOverlay && dom.modalOverlay.classList.contains('is-open')) {
                behaviors.closeVideoModal?.();
            }
        });
    }

    // --- WebSocket de carpeta: recibir init/refresh y re-renderizar items ---
    function setupFolderWS() {
        try {
            const container = document.querySelector('.container[data-folder-id]');
            if (!container) return;
            const folderId = container.getAttribute('data-folder-id');
            if (!folderId) return;

            const loc = window.location;
            const proto = (loc.protocol === 'https:') ? 'wss:' : 'ws:';
            const wsUrl = proto + '//' + loc.host + '/ws/folder/' + encodeURIComponent(folderId);
            const ws = new WebSocket(wsUrl);

            ws.onmessage = function (event) {
                try {
                    const payload = JSON.parse(event.data);
                    if (!payload || !payload.type) return;
                    if (payload.type === 'init' || payload.type === 'refresh') {
                        if (Array.isArray(payload.items)) {
                            renderFolderItems(payload.items);
                            if (window.refreshLazyChatPhotos) window.refreshLazyChatPhotos();
                        }
                    }
                } catch (e) {
                    console.error('WS folder message parse error', e);
                }
            };

            function renderFolderItems(items) {
                const grid = document.querySelector('.grid-view');
                if (!grid) return;
                if (typeof window.renderItems === 'function') {
                    window.renderItems(items);
                    if (window.refreshLazyChatPhotos) window.refreshLazyChatPhotos();
                    return;
                }
                grid.innerHTML = '<div style="padding:12px;">' + (items.length || 0) + ' elementos recibidos (WS)</div>';
            }
        } catch (e) {
            console.warn('WS folder setup error', e);
        }
    }

    // --- Sincronización de Carpeta (WebSocket) ---
    function setupLiveFolderUpdates() {
        const container = document.querySelector('.container[data-folder-id]');
        if (!container) return;

        const folderId = container.getAttribute('data-folder-id');
        if (!folderId) return;

        const grid = container.querySelector('.grid-view');
        if (!grid) return;

        function escapeHtml(str) {
            if (!str) return '';
            return String(str)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        function renderItems(items) {
            if (!Array.isArray(items)) return;
            let html = '';

            items.forEach(function (item) {
                const name = escapeHtml(item.name || '');
                const count = escapeHtml(item.count || '');
                const link = item.link || '#';
                const type = item.type || 'chat';
                const photoId = item.photo_id;
                const photoUrl = item.photo_url || (photoId ? ('/api/photo/' + encodeURIComponent(photoId) + '?tipo=grupo') : null);
                const telegramLink = item.telegram_link || '';
                const placeholder = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==';

                if (type === 'video') {
                    const videoId = item.video_id || '';
                    const chatId = item.chat_id || '';
                    const durationText = item.duration_text || '';
                    const streamUrl = item.stream_url || (item.link || '').replace('/play/', '/video_stream/');

                    html += '<a href="' + link + '" class="file-item" data-item-type="video"'
                        + ' data-stream-url="' + escapeHtml(streamUrl) + '"'
                        + (videoId ? ' data-video-id="' + escapeHtml(videoId) + '"' : '')
                        + (chatId ? ' data-chat-id="' + escapeHtml(chatId) + '"' : '')
                        + '>';

                    html += '<div class="icon-box video-box">'
                        + '<div class="video-hidden-indicator" title="Marcado como oculto">'
                        + '<i class="fas fa-eye-slash"></i>'
                        + '</div>'
                        + '<div class="video-loading-spinner" aria-hidden="true">'
                        + '<i class="fas fa-circle-notch fa-spin"></i>'
                        + '</div>';

                    if (photoId) {
                        html += '<img src="/api/photo/' + encodeURIComponent(photoId) + '?chat_id='
                            + encodeURIComponent(chatId) + '&video_id=' + encodeURIComponent(videoId)
                            + '" class="video-thumb" loading="lazy">';
                    } else {
                        html += '<i class="fas fa-file-video file-video"></i>';
                    }

                    if (durationText) {
                        html += '<div class="video-duration">' + escapeHtml(durationText) + '</div>';
                    }

                    html += '</div>'
                        + '<div class="file-name" title="' + name + '">' + name + '</div>'
                        + '<div class="file-info">' + count + '</div>'
                        + '</a>';
                } else {
                    html += '<a href="' + link + '" class="file-item" data-item-type="' + escapeHtml(type) + '">';

                    if (telegramLink) {
                        html += '<span class="btn-open-telegram" data-telegram-link="' + escapeHtml(telegramLink) + '" title="Abrir en Telegram">'
                            + '<i class="fa-brands fa-telegram"></i>'
                            + '</span>';
                    }

                    html += '<div class="icon-box folder-box">'
                        + '<i class="fas fa-folder folder-chat folder-icon"></i>';
                    if (photoUrl) {
                        html += '<img src="' + placeholder + '" data-src="' + escapeHtml(photoUrl)
                            + '" class="profile-img folder-overlay lazy-chat-photo" loading="lazy">';
                    }
                    html += '</div>'
                        + '<div class="file-name" title="' + name + '">' + name + '</div>'
                        + '<div class="file-info">' + count + '</div>'
                        + '</a>';
                }
            });

            grid.innerHTML = html;
            behaviors.applyHiddenStateToAll?.();
            behaviors.applyDuplicateFilter?.();
            behaviors.applyWatchLaterStateToAll?.();
            behaviors.setupLazyChatPhotos?.();
            behaviors.setupHoverPreviews?.();
        }

        // Exponer para reutilizar desde otros listeners (WS init/refresh)
        window.renderItems = renderItems;

        async function fetchAndRender() {
            try {
                const res = await fetch('/api/folder/' + encodeURIComponent(folderId));
                if (!res.ok) {
                    console.error('Error recargando carpeta', res.status);
                    return;
                }
                const data = await res.json();
                renderItems(data);
            } catch (e) {
                console.error('Error fetch /api/folder', e);
            }
        }

        const loc = window.location;
        const proto = (loc.protocol === 'https:') ? 'wss:' : 'ws:';
        const wsUrl = proto + '//' + loc.host + '/ws/folder/' + encodeURIComponent(folderId);

        let ws;
        try {
            ws = new WebSocket(wsUrl);
        } catch (e) {
            console.error('No se pudo abrir WebSocket de carpeta', e);
            return;
        }

        ws.onmessage = function (event) {
            try {
                const payload = JSON.parse(event.data);
                if (payload && payload.type === 'refresh') {
                    fetchAndRender();
                }
            } catch (e) {
                console.error('Mensaje WS inválido', e);
            }
        };

        window.addEventListener('beforeunload', function () {
            try {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.close();
                }
            } catch (e) {}
        });
    }

    function init() {
        behaviors.setupHoverPreviews?.();
        setupLiveFolderUpdates();
        behaviors.applyHiddenStateToAll?.();
        behaviors.applyDuplicateFilter?.();
        behaviors.applyWatchLaterStateToAll?.();
        behaviors.setupLazyChatPhotos?.();
        behaviors.initSearchHandlers?.();
        setupFolderWS();
        bindGlobalEvents();

        const btnSync = document.getElementById("btn-sync-diario");
        if (btnSync) {
            btnSync.addEventListener("click", async () => {
                btnSync.disabled = true;
                btnSync.textContent = "Sincronizando...";
                try {
                    await fetch("/sync/diario", { method: "POST" });
                    alert("Sincronización completada");
                } catch (e) {
                    console.error("Error sync diario", e);
                    alert("Error al sincronizar");
                } finally {
                    btnSync.disabled = false;
                    btnSync.textContent = "Sincronizar diario";
                }
            });
        }
    }

    init();
})();
