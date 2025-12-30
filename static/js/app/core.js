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
    // Carga un video preview al hacer hover sobre cards de video.
    function setupHoverPreviews() {
        const cards = document.querySelectorAll('.file-item[data-item-type="video"]');
        cards.forEach(function (card) {
            if (card.dataset.hoverInitialized === '1') return;
            card.dataset.hoverInitialized = '1';

            card.addEventListener('mouseenter', function () {
                if (card.dataset.previewActive === '1' || card.dataset.previewLoading === '1') return;
                const streamUrl = card.dataset.streamUrl;
                if (!streamUrl) return;

                const box = card.querySelector('.icon-box.video-box');
                if (!box) return;

                const spinner = box.querySelector('.video-loading-spinner');
                if (spinner) spinner.style.display = 'flex';

                card.dataset.previewLoading = '1';

                const timerId = setTimeout(function () {
                    if (!card.matches(':hover')) {
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
                        if (!card.matches(':hover')) return;
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
            });

            card.addEventListener('mouseleave', function () {
                const timerId = card.dataset.previewTimer;
                if (timerId) {
                    clearTimeout(Number(timerId));
                    delete card.dataset.previewTimer;
                }

                const box = card.querySelector('.icon-box.video-box');
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
            });
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
        updateWatchLaterButton,
        updateHideVideoButton,
        applyDuplicateFilter,
        setupHoverPreviews,
        setupLazyChatPhotos,
        saveHiddenVideos,
    });
})();
