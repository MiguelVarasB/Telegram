(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state || {};
    const behaviors = App.behaviors || {};
    const btnSyncFaltantes = document.getElementById('btn-sync-faltantes');
    state.batchScan = state.batchScan || { jobId: null, total: 0, done: 0, running: 0 };

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
    // --- Event Listeners Globales ---
    // Conecta botones, toggles y delegación de clicks a behaviors ya definidos.
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

        if (dom.videoEditSaveBtn) {
            dom.videoEditSaveBtn.addEventListener('click', behaviors.saveVideoMetadata);
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

        if (btnSyncFaltantes) {
            btnSyncFaltantes.addEventListener('click', behaviors.triggerBatchScan);
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
    // Escucha mensajes en tiempo real para reemplazar la grilla de la carpeta.
    function setupFolderWS() {
        try {
            const container = document.querySelector('.container[data-folder-id]');
            if (!container) return;
            const folderId = container.getAttribute('data-folder-id');
            if (!folderId) return;
            // Solo la carpeta especial -1 usa este WS de init
            if (folderId !== '-1') return;

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
                    if (payload.type === 'batch_scan_start') {
                        state.batchScan = {
                            jobId: payload.job_id,
                            total: payload.total || 0,
                            done: 0,
                            running: 0,
                        };
                        updateBatchButton(formatBatchLabel(), true);
                    }
                    if (payload.type === 'batch_scan_update' && state.batchScan.jobId === payload.job_id) {
                        state.batchScan.done = payload.done ?? state.batchScan.done;
                        state.batchScan.running = payload.running ?? state.batchScan.running;
                        updateBatchButton(formatBatchLabel(), true);
                    }
                    if (payload.type === 'batch_scan_done' && state.batchScan.jobId === payload.job_id) {
                        state.batchScan.done = payload.done ?? state.batchScan.done;
                        state.batchScan.running = 0;
                        updateBatchButton('Sincronizar faltantes', false);
                    }
                    if (payload.type === 'scan_done' && payload.chat_id) {
                        // Si el modal actual corresponde al chat, actualiza
                        if (window.App?.state?.channelModalData?.chatId == payload.chat_id) {
                            if (typeof window.App?.behaviors?.fillChannelMeta === 'function') {
                                window.App.behaviors.fillChannelMeta({
                                    type: window.App.state.channelModalData.chatType,
                                    username: window.App.state.channelModalData.username,
                                    indexed_videos: payload.indexed_videos,
                                    total_videos: payload.total_videos,
                                    scanned_at: new Date().toISOString(),
                                    last_message_date: window.App.state.channelModalData.lastMessage,
                                });
                            }
                            const statusEl = document.getElementById('channel-modal-status');
                            if (statusEl) statusEl.textContent = 'Escaneo finalizado';
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
    // Conecta a ws/folder/:id y, en refresh, refetch de /api/folder para re-renderizar la grilla.
    function setupLiveFolderUpdates() {
        const container = document.querySelector('.container[data-folder-id]');
        if (!container) return;

        const folderId = container.getAttribute('data-folder-id');
        if (!folderId) return;

        const grid = container.querySelector('.grid-view');
        if (!grid) return;

        // Escapado simple para valores renderizados en HTML
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

            // Construye cards de chat o video según el tipo recibido
            items.forEach(function (item) {
               
                const folderId = item.folder_id || '';
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
                    html += '<a href="' + link + '" class="file-item"'
                        + ' data-item-type="' + escapeHtml(type) + '"'
                        + ' data-chat-id="' + escapeHtml(chatId) + '"'
                        + ' data-faltantes="' + escapeHtml(faltantes) + '"'
                        + ' data-indexed="' + escapeHtml(indexed) + '"'
                        + ' data-total="' + escapeHtml(total) + '"'
                        + ' data-scanned-at="' + escapeHtml(scannedAt) + '">';

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
                        + '<div class="file-info">' + count + '</div>';

                    if (folderId) {
                        html += '<button type="button"'
                            + ' class="btn-sync open-channel-modal"'
                            + ' onclick="return window.openChannelModalFromButton && window.openChannelModalFromButton(this);"'
                            + ' data-chat-id="' + escapeHtml(chatId) + '"'
                            + ' data-chat-name="' + name + '"'
                            + ' data-indexed="' + escapeHtml(indexed) + '"'
                            + ' data-total="' + escapeHtml(total) + '"'
                            + ' data-scanned-at="' + escapeHtml(scannedAt) + '"'
                            + ' data-chat-type="' + escapeHtml(chatType) + '"'
                            + ' data-last-message="' + escapeHtml(lastMsg) + '"'
                            + ' data-username="' + escapeHtml(username) + '"'
                            + ' data-telegram-link="' + escapeHtml(telegramLink) + '">'
                            + 'Info / Indexar'
                            + '</button>';
                    }
                    if(folderId==-1){
                        btnSyncFaltantes.style.display='';
                    }

                    html += '</a>';
                }
            });

            grid.innerHTML = html;
            if (items.some((it) => it.folder_id)) {
                grid.querySelectorAll('.btn-sync.open-channel-modal').forEach((btn) => {
                    btn.style.display = '';
                });
            }
            if (window.refreshLazyChatPhotos) window.refreshLazyChatPhotos();
            behaviors.applyHiddenStateToAll?.();
            behaviors.applyDuplicateFilter?.();
            behaviors.applyWatchLaterStateToAll?.();
            behaviors.setupLazyChatPhotos?.();
            behaviors.setupHoverPreviews?.();
        }

        // Exponer renderItems globalmente para que setupFolderWS pueda reutilizarlo (carpeta -1)
        window.renderItems = renderItems;

        // Para la carpeta especial -1 evitamos abrir este WS (lo maneja setupFolderWS)
        // pero dejamos renderItems disponible.
        if (folderId === '-1') {
            return;
        }

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
        const limitInput = document.getElementById('search-limit');
        const urlParams = new URLSearchParams(loc.search);
        const limitFromUrl = urlParams.get('limite') || urlParams.get('limit');
        const limitValue = (limitInput && limitInput.value) || limitFromUrl || '';
        const wsQuery = limitValue ? ('?limite=' + encodeURIComponent(limitValue)) : '';
        const wsUrl = proto + '//' + loc.host + '/ws/folder/' + encodeURIComponent(folderId) + wsQuery;

        let ws;
        try {
            ws = new WebSocket(wsUrl);
        } catch (e) {
            console.error('No se pudo abrir WebSocket de carpeta', e);
            return;
        }

        // Cada mensaje "refresh" dispara un refetch de la carpeta
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

        // Primera carga inmediata vía API
        fetchAndRender();
    }

    // --- Trigger batch scan desde el front (usa el nuevo endpoint) ---
    behaviors.triggerBatchScan = async function triggerBatchScan() {
        const container = document.querySelector('.container[data-folder-id]');
        const folderId = container?.getAttribute('data-folder-id') || null;
        const cards = Array.from(document.querySelectorAll('.file-item[data-chat-id]'))
            .filter((el) => Number(el.dataset.faltantes || 0) > 0)
            // Orden: menos videos totales primero (fallback a faltantes, luego chat_id)
            .sort((a, b) => {
                const ta = Number(a.dataset.total || 0);
                const tb = Number(b.dataset.total || 0);
                if (ta !== tb) return ta - tb;
                const fa = Number(a.dataset.faltantes || 0);
                const fb = Number(b.dataset.faltantes || 0);
                if (fa !== fb) return fa - fb;
                return Number(a.dataset.chatId || 0) - Number(b.dataset.chatId || 0);
            });
        if (!btnSyncFaltantes) return;
        if (!cards.length) {
            updateBatchButton('Nada que sincronizar', true);
            setTimeout(() => updateBatchButton('Sincronizar faltantes', false), 1500);
            return;
        }
        const chatIds = cards.map((el) => Number(el.dataset.chatId)).filter((n) => Number.isFinite(n));
        updateBatchButton(`Enviando (${chatIds.length})...`, true);
        try {
            const res = await fetch('/api/folder/scan-batch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    chat_ids: chatIds,
                    folder_id: folderId ? Number(folderId) : null,
                }),
            });
            if (!res.ok) throw new Error('scan-batch failed');
            const data = await res.json();
            state.batchScan = {
                jobId: data.job_id,
                total: data.total || chatIds.length,
                done: 0,
                running: 0,
            };
            updateBatchButton(formatBatchLabel(), true);
        } catch (e) {
            console.error('Error batch scan', e);
            updateBatchButton('Error al iniciar', true);
            setTimeout(() => updateBatchButton('Sincronizar faltantes', false), 2000);
        }
    };

    // --- Inicialización principal ---
    // Aplica estados iniciales, listeners y WS al cargar la página.
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
        updateBatchButton('Sincronizar faltantes', false);
    }

    init();
})();
async function toggleWatchLater(e, itemId) {
    // Evita que el click en el botón dispare el link del contenedor
    e.preventDefault();
    e.stopPropagation();

    const card = e.currentTarget.closest('.file-item');
    const behaviors = window.App?.behaviors;

    const current = card?.dataset.watchLater === '1';
    const nextValue = !current;

    try {   
        const res = await fetch(`/api/video/${encodeURIComponent(itemId)}/watch_later`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ value: nextValue }),
        });
        const text = await res.text();
        if (!res.ok) throw new Error(`watch_later ${res.status}: ${text}`);
        let data;
        try { data = JSON.parse(text); } catch { data = {}; }
        const saved = data.watch_later ? '1' : '0';
        if (card) {
            card.dataset.watchLater = saved;
            behaviors?.applyWatchLaterStateToElement?.(card);
        }

    } catch (err) {
        console.error('Error al marcar ver después', err);
        alert('No se pudo actualizar "ver más tarde". Intenta nuevamente.');
    }
}