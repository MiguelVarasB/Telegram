(function () {
    // --- Instrumentación de rendimiento en cliente ---
    // Mide tiempos de carga (nav → DOM ready → load) y los loguea en consola.
    try {
        const nav = performance.getEntriesByType('navigation')[0];
        if (nav) {
            console.log('[perf] navigation', {
                domContentLoaded: nav.domContentLoadedEventEnd,
                loadEvent: nav.loadEventEnd,
                requestStart: nav.requestStart,
                responseEnd: nav.responseEnd,
                duration: nav.duration,
            });
        }

        performance.mark('app_js_init');
        window.addEventListener('DOMContentLoaded', () => {
            performance.mark('dom_ready');
            performance.measure('dom_to_app_js', 'app_js_init', 'dom_ready');
            const m = performance.getEntriesByName('dom_to_app_js')[0];
            if (m) console.log('[perf] dom_to_app_js', m.duration.toFixed(1), 'ms');
        });
        window.addEventListener('load', () => {
            performance.mark('window_load');
            performance.measure('nav_to_load', 'app_js_init', 'window_load');
            const m = performance.getEntriesByName('nav_to_load')[0];
            if (m) console.log('[perf] nav_to_load', m.duration.toFixed(1), 'ms');
        });
    } catch (e) {
        console.warn('[perf] instrumentation error', e);
    }
})();

(function () {
    const App = window.App = window.App || {};
    const dom = App.dom = App.dom || {};
    const state = App.state = App.state || {};
    const behaviors = App.behaviors = App.behaviors || {};
    const helpers = App.helpers = App.helpers || {};

    // --- Referencias DOM ---
    dom.modalOverlay = document.getElementById('video-modal-overlay');
    dom.modalVideo = document.getElementById('modal-video');
    dom.modalClose = document.getElementById('modal-close-btn');
    dom.detailsContainer = document.getElementById('video-details');
    dom.detailsTitle = document.getElementById('video-title');
    dom.detailsMetaLine = document.getElementById('video-meta-line');
    dom.detailsExtraLine = document.getElementById('video-extra-line');
    dom.detailsCaption = document.getElementById('video-caption');
    dom.detailsIds = document.getElementById('video-ids');
    dom.detailsWatchLaterPill = document.getElementById('video-watchlater-pill');
    dom.messagesContainer = document.getElementById('video-messages-container');
    dom.msgToggleBtn = document.getElementById('msg-toggle-btn');
    dom.msgToggleIcon = document.getElementById('msg-toggle-icon');
    dom.toggleDuplicates = document.getElementById('toggle-duplicates');
    dom.hideVideoButton = document.getElementById('btn-hide-video');
    dom.watchLaterButton = document.getElementById('btn-watch-later');
    dom.videoEditTitleInput = document.getElementById('video-edit-title');
    dom.videoEditDurationInput = document.getElementById('video-edit-duration');
    dom.videoEditSaveBtn = document.getElementById('video-edit-save');
    dom.videoEditStatus = document.getElementById('video-edit-status');
    dom.channelModalOverlay = document.getElementById('channel-modal-overlay') || null;
    dom.channelModalClose = document.getElementById('channel-modal-close') || null;
    dom.channelModalTitle = document.getElementById('channel-modal-title') || null;
    dom.channelModalMeta = document.getElementById('channel-modal-meta') || null;
    dom.channelModalStatus = document.getElementById('channel-modal-status') || null;
    dom.channelModalOpenBtn = document.getElementById('channel-modal-open') || null;
    dom.channelModalScanBtn = document.getElementById('channel-modal-scan') || null;
    dom.statsButton = document.getElementById('btn-stats');
    dom.statsOverlay = document.getElementById('stats-modal-overlay');
    dom.statsClose = document.getElementById('stats-modal-close-btn');
    dom.statsContent = document.getElementById('stats-modal-content');
    dom.downloadsButton = document.getElementById('btn-downloads');
    dom.downloadsOverlay = document.getElementById('downloads-modal-overlay');
    dom.downloadsClose = document.getElementById('downloads-modal-close-btn');
    dom.downloadsContent = document.getElementById('downloads-modal-content');
    dom.btnSyncFaltantes = document.getElementById('btn-sync-faltantes');

    // --- Lazy load de fotos de canal ---
    // Crea un observer para cargar imágenes de perfil cuando entran en viewport.
    function setupLazyChatPhotos(root) {
        try {
            const observer = new IntersectionObserver(function (entries, obs) {
                entries.forEach(function (entry) {
                    if (entry.isIntersecting) {
                        const img = entry.target;
                        const src = img.getAttribute('data-src');
                        if (src) {
                            img.src = src;
                            img.removeAttribute('data-src');
                        }
                        obs.unobserve(img);
                    }
                });
            }, { rootMargin: '200px' });

            const scope = root || document;
            const imgs = scope.querySelectorAll('.lazy-chat-photo[data-src]');
            imgs.forEach(function (img) { observer.observe(img); });

            window.refreshLazyChatPhotos = function (r) {
                const sc = r || document;
                const imgs2 = sc.querySelectorAll('.lazy-chat-photo[data-src]');
                imgs2.forEach(function (img) { observer.observe(img); });
            };
        } catch (e) {
            console.warn('lazy photos error', e);
        }
    }

    // --- Estado ---
    // Mantiene caché de hiddenVideos y referencias de selección actual.
    state.hiddenVideos = state.hiddenVideos || loadHiddenVideos();
    state.currentChannelId = (document.body?.dataset?.currentChannelId) || null;
    state.currentVideoElement = null;
    state.currentVideoId = null;

    // --- Helpers de formateo ---
    // Escapa texto y formatea números para reuso en otras vistas.
    function escapeHtmlStats(str) {
        if (str === null || str === undefined) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function formatThousands(value) {
        if (value === null || value === undefined) return '';
        const n = Number(value);
        if (!isFinite(n)) return String(value);
        try {
            return new Intl.NumberFormat('es-AR').format(Math.trunc(n));
        } catch (e) {
            return String(Math.trunc(n));
        }
    }

    // --- Videos ocultos ---
    // Lee/guarda sets de videos ocultos y aplica estado a la grilla.
    function loadHiddenVideos() {
        try {
            const raw = localStorage.getItem('hidden_videos');
            if (!raw) return new Set();
            const arr = JSON.parse(raw);
            if (!Array.isArray(arr)) return new Set();
            return new Set(arr);
        } catch (e) {
            return new Set();
        }
    }

    function saveHiddenVideos() {
        try {
            localStorage.setItem('hidden_videos', JSON.stringify(Array.from(state.hiddenVideos || [])));
        } catch (e) {}
    }

    function applyHiddenStateToAll() {
        const items = document.querySelectorAll('.file-item[data-item-type="video"]');
        items.forEach(function (el) {
            const vid = el.dataset.videoId || '';
            if (!vid) return;
            const isHidden = state.hiddenVideos.has(vid);
            el.dataset.hidden = isHidden ? '1' : '0';
            if (isHidden) {
                el.classList.add('is-hidden-video');
            } else {
                el.classList.remove('is-hidden-video');
            }
        });
    }

    // --- Ver más tarde ---
    // Pinta estado de "ver más tarde" tanto en DOM como en el botón principal.
    function applyWatchLaterStateToElement(el) {
        if (!el) return;
        const watchLaterFlag = el.dataset.watchLater === '1';
        if (watchLaterFlag) {
            el.classList.add('is-watchlater');
        } else {
            el.classList.remove('is-watchlater');
        }
        const indicator = el.querySelector('.video-watchlater-indicator');
        if (indicator) {
            indicator.style.display = watchLaterFlag ? 'flex' : 'none';
        }
    }

    function applyWatchLaterStateToAll() {
        const items = document.querySelectorAll('.file-item[data-item-type="video"]');
        items.forEach(function (el) { applyWatchLaterStateToElement(el); });
    }

    // --- Descargas en tarjetas ---
    state.cardDownloadsPoll = state.cardDownloadsPoll || null;
    state.emptyDownloadPolls = 0;

    // --- Reproducir local en Windows (sin abrir nueva página) ---
    async function openWithWindowsPlayer(btn) {
        if (!btn) return;
        const url = btn.getAttribute('href');
        if (!url) return;
        if (btn.dataset.opening === '1') return;
        btn.dataset.opening = '1';
        const originalHtml = btn.innerHTML;
        try {
            btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
            btn.style.opacity = '0.8';
            const res = await fetch(url, { method: 'GET' });
            if (!res.ok) {
                const msg = await res.text().catch(() => '');
                alert(`No se pudo abrir el reproductor (HTTP ${res.status}). ${msg || ''}`);
                return;
            }
            // No hacemos nada más: el endpoint ya lanza el reproductor en el servidor.
        } catch (e) {
            alert('No se pudo abrir el reproductor predeterminado.');
        } finally {
            btn.innerHTML = originalHtml;
            btn.style.opacity = '';
            delete btn.dataset.opening;
        }
    }

    document.addEventListener('click', function (ev) {
        const playBtn = ev.target.closest('.btn-play');
        if (!playBtn) return;
        // Evitar navegación; solo llamamos al endpoint
        ev.preventDefault();
        ev.stopPropagation();
        openWithWindowsPlayer(playBtn);
    });

    function applyDownloadStatusToCards(list) {
        if (!Array.isArray(list)) return;
        const downloadingIds = new Set(
            list.filter(d => d.status === 'downloading' && d.video_id).map(d => d.video_id)
        );
        const cards = document.querySelectorAll('.file-item[data-item-type="video"]');
        cards.forEach(function (card) {
            const vid = card.dataset.videoId;
            const row = card.querySelector('[data-download-status]');
            const btn = card.querySelector('.btn-download[data-action="download"]');
            if (!row) return;
            if (downloadingIds.has(vid)) {
                row.style.display = 'flex';
                row.dataset.downloading = '1';
                if (btn) btn.style.display = 'none';
            } else if (row.dataset.downloading) {
                row.style.display = 'none';
                delete row.dataset.downloading;
                if (btn) btn.style.display = '';
            }
        });
        if (downloadingIds.size === 0) {
            state.manualDownloadStarted = false;
        }
    }

    async function pollDownloadsForCards() {
        try {
            const res = await fetch('/api/downloads/status');
            if (!res.ok) return;
            const data = await res.json();
            if (!data || data.length === 0) {
                state.emptyDownloadPolls = (state.emptyDownloadPolls || 0) + 1;
                const shouldStop = !state.manualDownloadStarted && state.emptyDownloadPolls >= 3;
                if (shouldStop) {
                    stopCardDownloadsPolling();
                }
            } else {
                state.emptyDownloadPolls = 0;
                applyDownloadStatusToCards(data);
            }
        } catch (e) {
            // silencioso
            stopCardDownloadsPolling();
        }
    }

    function startCardDownloadsPolling() {
        if (state.cardDownloadsPoll) return;
        pollDownloadsForCards();
        state.cardDownloadsPoll = setInterval(pollDownloadsForCards, 2000);
    }

    function stopCardDownloadsPolling() {
        if (state.cardDownloadsPoll) {
            clearInterval(state.cardDownloadsPoll);
            state.cardDownloadsPoll = null;
        }
        state.manualDownloadStarted = false;
        state.emptyDownloadPolls = 0;
    }

    // --- Descarga completa ---
    async function startDownload(card, btn) {
        if (!card) return;
        const chatId = card.dataset.chatId;
        const messageId = card.dataset.messageId;
        const videoId = card.dataset.videoId;
        const videoName = card.dataset.name;
        if (!chatId || !messageId || !videoId) {
            console.warn('Faltan datos para descargar', { chatId, messageId, videoId });
            return;
        }
        state.manualDownloadStarted = true;
        state.emptyDownloadPolls = 0;
        startCardDownloadsPolling();

        const statusRow = card.querySelector('[data-download-status]');
        const originalBtnHtml = btn ? btn.innerHTML : null;
        const originalBtnDisplay = btn ? btn.style.display : null;

        if (btn) {
            btn.disabled = true;
            btn.classList.add('is-loading');
            btn.setAttribute('aria-busy', 'true');
            btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
            btn.style.display = 'none';
        }
        if (statusRow) {
            statusRow.style.display = 'flex';
            statusRow.dataset.downloading = '1';
        }
        const cancelBtn = card.querySelector('.btn-cancel-download');
        if (cancelBtn) {
            cancelBtn.disabled = false;
            cancelBtn.style.display = 'inline-flex';
            cancelBtn.onclick = async (ev) => {
                ev.stopPropagation();
                cancelBtn.disabled = true;
                try {
                    await fetch(`/api/downloads/cancel/${chatId}/${messageId}/${videoId}`, { method: 'POST' });
                } catch (e) {
                    // silencioso
                } finally {
                    cancelBtn.disabled = false;
                }
            };
        }

        try {
            const params = new URLSearchParams({ video_id: videoId });
            if (videoName) params.append('video_name', videoName);
            const res = await fetch(`/api/download/${chatId}/${messageId}?${params.toString()}`, { method: 'POST' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            if (data.status === 'ready') {
                window.open(`/api/download/file/${chatId}/${videoId}`, '_blank');
                if (statusRow) {
                    statusRow.style.display = 'none';
                    delete statusRow.dataset.downloading;
                }
            } else {
                behaviors.openDownloadsModal?.();
            }
        } catch (e) {
            console.warn('No se pudo iniciar la descarga', e);
            alert('No se pudo iniciar la descarga. Intenta nuevamente.');
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.classList.remove('is-loading');
                btn.removeAttribute('aria-busy');
                if (originalBtnHtml) btn.innerHTML = originalBtnHtml;
                if (originalBtnDisplay !== null) btn.style.display = originalBtnDisplay;
            }
            if (statusRow && btn?.disabled === false) {
                // Si falló, ocultamos el indicador
                statusRow.style.display = 'none';
                delete statusRow.dataset.downloading;
            }
            const cancelBtn2 = card.querySelector('.btn-cancel-download');
            if (cancelBtn2) cancelBtn2.style.display = 'none';
        }
    }

    function updateWatchLaterButton() {
        if (!dom.watchLaterButton) return;
        if (!state.currentVideoId || !state.currentVideoElement) {
            dom.watchLaterButton.style.display = 'none';
            return;
        }
        dom.watchLaterButton.style.display = 'inline-flex';
        const isSaved = state.currentVideoElement.dataset.watchLater === '1';
        const icon = dom.watchLaterButton.querySelector('i');
        const span = dom.watchLaterButton.querySelector('span');
        if (icon) {
            icon.classList.toggle('fa-regular', !isSaved);
            icon.classList.toggle('fa-solid', isSaved);
        }
        if (span) {
            span.textContent = isSaved ? 'Quitar de ver más tarde' : 'Guardar para ver más tarde';
        }
    }

    async function toggleWatchLaterFromModal(ev) {
        if (ev) ev.preventDefault();
        const videoId = state.currentVideoId;
        const card = state.currentVideoElement;
        if (!videoId) return;
        const current = card?.dataset.watchLater === '1';
        const nextValue = !current;
        try {
            const res = await fetch(`/api/video/${encodeURIComponent(videoId)}/watch_later`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ value: nextValue }),
            });
            if (!res.ok) throw new Error(`watch_later ${res.status}`);
            const data = await res.json();
            const saved = data.watch_later ? '1' : '0';
            if (card) {
                card.dataset.watchLater = saved;
                applyWatchLaterStateToElement(card);
            }
            if (dom.detailsWatchLaterPill) {
                dom.detailsWatchLaterPill.style.display = saved === '1' ? 'block' : 'none';
            }
            updateWatchLaterButton();
        } catch (err) {
            console.error('Error al marcar ver después (modal)', err);
            alert('No se pudo actualizar "ver más tarde". Intenta nuevamente.');
        }
    }

    // Bind de botón en modal
    if (dom.watchLaterButton) {
        dom.watchLaterButton.addEventListener('click', toggleWatchLaterFromModal);
    }

    function updateHideVideoButton() {
        if (!dom.hideVideoButton) return;
        if (!state.currentVideoId) {
            dom.hideVideoButton.style.display = 'none';
            return;
        }
        dom.hideVideoButton.style.display = 'inline-flex';
        const isHidden = state.hiddenVideos.has(state.currentVideoId);
        const icon = dom.hideVideoButton.querySelector('i');
        const span = dom.hideVideoButton.querySelector('span');
        if (icon) {
            if (isHidden) {
                icon.classList.remove('fa-eye-slash');
                icon.classList.add('fa-eye');
            } else {
                icon.classList.remove('fa-eye');
                icon.classList.add('fa-eye-slash');
            }
        }
        if (span) {
            span.textContent = isHidden ? 'Quitar oculto' : 'Marcar como oculto';
        }
    }

    // --- Duplicados ---
    // Oculta duplicados cuando toggle está activado, manteniendo uno visible.
    function applyDuplicateFilter() {
        if (!dom.toggleDuplicates) return;
        const hideMode = dom.toggleDuplicates.checked;
        const seen = new Set();
        const items = document.querySelectorAll('.file-item[data-item-type="video"]');
        items.forEach(function (el) {
            const vid = el.dataset.videoId || '';
            const isHidden = el.dataset.hidden === '1';
            applyWatchLaterStateToElement(el);
            if (!hideMode) {
                el.style.display = '';
                return;
            }
            if (isHidden) {
                el.style.display = 'none';
                if (vid) seen.add(vid);
                return;
            }
            if (!vid) {
                el.style.display = '';
                return;
            }
            if (seen.has(vid)) {
                el.style.display = 'none';
            } else {
                seen.add(vid);
                el.style.display = '';
            }
        });
    }

    // --- Hover Previews ---
    // Carga un video preview al hacer hover solo sobre el link del thumb.
    function setupHoverPreviews() {
        const links = document.querySelectorAll('.video-thumb-link');
        links.forEach(function (link) {
            if (link.dataset.hoverInitialized === '1') return;
            link.dataset.hoverInitialized = '1';

            const card = link.closest('.file-item[data-item-type="video"]');
            if (!card) return;

            function startHover() {
                if (card.dataset.previewActive === '1' || card.dataset.previewLoading === '1') return;
                const streamUrl = card.dataset.streamUrl;
                if (!streamUrl) return;

                const box = link.querySelector('.icon-box.video-box');
                if (!box) return;

                const spinner = box.querySelector('.video-loading-spinner');
                if (spinner) spinner.style.display = 'flex';

                card.dataset.previewLoading = '1';

                const timerId = setTimeout(function () {
                    if (!link.matches(':hover')) {
                        card.dataset.previewLoading = '0';
                        if (spinner) spinner.style.display = 'none';
                        return;
                    }

                    if (box.querySelector('video.video-preview')) return;

                    const preview = document.createElement('video');
                    preview.className = 'video-preview';
                    preview.src = streamUrl;
                    preview.muted = true;
                    preview.autoplay = true;
                    preview.loop = true;
                    preview.playsInline = true;
                    preview.playbackRate = 2.0;

                    preview.addEventListener('loadeddata', function () {
                        if (!link.matches(':hover')) return;
                        preview.classList.add('is-visible');
                        if (spinner) spinner.style.display = 'none';
                        card.dataset.previewLoading = '0';
                    });

                    preview.addEventListener('error', function () {
                        if (spinner) spinner.style.display = 'none';
                        card.dataset.previewLoading = '0';
                    });

                    box.appendChild(preview);
                    card.dataset.previewActive = '1';
                }, 500);

                card.dataset.previewTimer = String(timerId);
            }

            function stopHover() {
                const timerId = card.dataset.previewTimer;
                if (timerId) {
                    clearTimeout(Number(timerId));
                    delete card.dataset.previewTimer;
                }

                const box = link.querySelector('.icon-box.video-box');
                if (box) {
                    const preview = box.querySelector('video.video-preview');
                    if (preview) {
                        try { preview.pause(); } catch (e) {}
                        preview.remove();
                    }
                    const spinner = box.querySelector('.video-loading-spinner');
                    if (spinner) spinner.style.display = 'none';
                }

                card.dataset.previewActive = '0';
                card.dataset.previewLoading = '0';
            }

            link.addEventListener('mouseenter', startHover);
            link.addEventListener('mouseleave', stopHover);
        });
    }

    App.helpers = Object.assign({}, App.helpers, {
        formatThousands,
        escapeHtmlStats,
    });

    App.behaviors = Object.assign({}, App.behaviors, {
        applyHiddenStateToAll,
        applyWatchLaterStateToElement,
        applyWatchLaterStateToAll,
        applyDownloadStatusToCards,
        startCardDownloadsPolling,
        updateWatchLaterButton,
        updateHideVideoButton,
        applyDuplicateFilter,
        setupHoverPreviews,
        setupLazyChatPhotos,
        saveHiddenVideos,
    });

    // Delegación: click en botón Descargar de cada card
    document.addEventListener('click', function (event) {
        const btn = event.target.closest('.btn-download');
        if (!btn) return;
        const card = btn.closest('.file-item[data-item-type="video"]');
        if (!card) return;
        event.preventDefault();
        startDownload(card, btn);
    });
})();
