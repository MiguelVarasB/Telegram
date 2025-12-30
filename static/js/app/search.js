(function () {
    const App = window.App = window.App || {};
    const state = App.state || {};
    const helpers = App.helpers || {};
    const behaviors = App.behaviors = App.behaviors || {};

    // Inicializa handlers de búsqueda y renderizado de resultados
    function initSearchHandlers() {
        const searchForm = document.getElementById('search-form');
        if (!searchForm) return;

        const searchInput = document.getElementById('search-query');
        const scopeSelect = document.getElementById('search-scope');
        const typeSelect = document.getElementById('search-type');
        let limitInput = document.getElementById('search-limit');

        // Ejecuta búsqueda contra /api/search y re-renderiza la grilla
        async function performSearch(ev) {
            if (ev) ev.preventDefault();
            const q = (searchInput?.value || '').trim();
            const scope = scopeSelect?.value || 'local';
            const type = typeSelect?.value || 'video';
            const limitRaw = limitInput?.value;
            const limit = Math.min(Math.max(parseInt(limitRaw || '20', 10) || 20, 1), 1000);

            console.log('[search] submit', { q, scope, type, limit, currentChannelId: state.currentChannelId });
            if (!q) return;

            const grid = document.querySelector('.grid-view');
            const title = document.querySelector('.section-title');
            const paginationBar = document.querySelector('.pagination-bar');
            if (!grid) console.warn('[search] No se encontró grid-view para renderizar resultados');
            if (title) title.textContent = 'Buscando...';
            if (grid) grid.innerHTML = '<div style="padding:12px;">Cargando...</div>';
            if (paginationBar) paginationBar.remove();

            try {
                const params = new URLSearchParams({ q, scope, type, limit: String(limit) });
                if (state.currentChannelId) params.set('chat_id', state.currentChannelId);

                const url = new URL(window.location.href);
                params.forEach((val, key) => url.searchParams.set(key, val));
                history.replaceState(null, '', url.toString());

                const res = await fetch('/api/search?' + params.toString());
                if (!res.ok) throw new Error('Error búsqueda');
                const data = await res.json();
                const items = data.items || [];
                console.log('[search] resultados', items.length);

                let resultsTitle = document.getElementById('results-title');
                let limitContainerDyn = document.getElementById('limites');
                const titleBar = resultsTitle ? resultsTitle.parentElement : document.querySelector('.section-title');
                const resultsText = `Resultados de la busqueda (${helpers.formatThousands(items.length)})`;

                if (!limitContainerDyn && titleBar) {
                    limitContainerDyn = document.createElement('div');
                    limitContainerDyn.id = 'limites';
                    limitContainerDyn.className = 'results-limit-control';
                    limitContainerDyn.style.display = 'inline-flex';
                    limitContainerDyn.innerHTML = `
                        <label for="search-limit">Límite</label>
                        <input type="number" id="search-limit" name="limit" min="1" max="10000" value="${limit}" step="100" inputmode="numeric">
                    `;
                    titleBar.appendChild(limitContainerDyn);
                    limitInput = document.getElementById('search-limit');
                } else if (limitContainerDyn) {
                    limitContainerDyn.style.display = 'inline-flex';
                }
                if (limitInput && !limitInput.value) limitInput.value = String(limit);

                if (resultsTitle) {
                    resultsTitle.textContent = resultsText;
                } else if (title) {
                    title.textContent = resultsText;
                }
                if (!grid) return;

                let html = '';
                if (type === 'video') {
                    items.forEach((item) => {
                        const link = `/play/${encodeURIComponent(item.chat_id || '')}/${encodeURIComponent(item.message_id || '')}`;
                        const durationText = item.duration ? `${Math.floor(item.duration / 60)}:${(item.duration % 60).toString().padStart(2, '0')}` : '';
                        const name = (item.text || item.file_name || 'Video').replace(/</g, '&lt;').replace(/>/g, '&gt;');
                        const sizeTxt = item.file_size ? `${(item.file_size / (1024 * 1024)).toFixed(1)} MB` : '';
                        html += '<a href="' + link + '" class="file-item" data-item-type="video"'
                            + ' data-stream-url="' + link.replace('/play/', '/video_stream/') + '"'
                            + (item.id ? ' data-video-id="' + String(item.id) + '"' : '')
                            + (item.chat_id ? ' data-chat-id="' + String(item.chat_id) + '"' : '')
                            + ' data-watch-later="' + (item.watch_later ? '1' : '0') + '"'
                            + (item.message_id ? ' data-message-id="' + String(item.message_id) + '"' : '')
                            + (item.mime_type ? ' data-mime="' + String(item.mime_type) + '"' : '')
                            + (item.date ? ' data-date="' + String(item.date) + '"' : '')
                            + (item.text ? ' data-caption="' + String(item.text).replace(/"/g, '&quot;') + '"' : '')
                            + '>';
                        html += '<div class="icon-box video-box">'
                            + '<div class="video-hidden-indicator" title="Marcado como oculto"><i class="fas fa-eye-slash"></i></div>'
                            + '<div class="video-watchlater-indicator" title="Guardado para ver más tarde"><i class="fas fa-clock"></i></div>'
                            + '<div class="video-loading-spinner" aria-hidden="true"><i class="fas fa-circle-notch fa-spin"></i></div>'
                            + '<i class="fas fa-file-video file-video"></i>';
                        if (durationText) html += '<div class="video-duration">' + durationText + '</div>';
                        html += '</div>'
                            + '<div class="file-name" title="' + name + '">' + name + '</div>'
                            + '<div class="file-info">' + (sizeTxt || '') + '</div>'
                            + '</a>';
                    });
                } else if (type === 'chat') {
                    items.forEach((item) => {
                        const link = `/channel/${encodeURIComponent(item.chat_id || '')}`;
                        const name = (item.name || item.chat_id || 'Chat').toString().replace(/</g, '&lt;').replace(/>/g, '&gt;');
                        html += '<a href="' + link + '" class="file-item" data-item-type="chat">'
                            + '<div class="icon-box folder-box"><i class="fas fa-folder folder-chat folder-icon"></i></div>'
                            + '<div class="file-name" title="' + name + '">' + name + '</div>'
                            + '<div class="file-info">' + (item.username ? '@' + item.username : '') + '</div>'
                            + '</a>';
                    });
                } else {
                    items.forEach((item) => {
                        const text = (item.text || item.file_name || '').toString().replace(/</g, '&lt;').replace(/>/g, '&gt;');
                        const info = `chat ${item.chat_id || ''} · msg ${item.message_id || ''}`;
                        html += '<div class="file-item" style="cursor:default;">'
                            + '<div class="file-name" title="' + text + '">' + text + '</div>'
                            + '<div class="file-info">' + info + '</div>'
                            + '</div>';
                    });
                }

                grid.innerHTML = html || '<div style="padding:12px;">Sin resultados</div>';
                behaviors.applyHiddenStateToAll?.();
                behaviors.applyDuplicateFilter?.();
                behaviors.applyWatchLaterStateToAll?.();
                behaviors.setupHoverPreviews?.();
            } catch (e) {
                if (title) title.textContent = 'Error en la búsqueda';
                if (grid) grid.innerHTML = '<div style="padding:12px;">Error al buscar</div>';
                console.error(e);
            }
        }

        searchForm.addEventListener('submit', performSearch);

        // Si la URL ya trae query, inicializa formulario y dispara búsqueda
        const initialParams = new URLSearchParams(window.location.search || '');
        const initialQ = initialParams.get('q');
        if (initialQ) {
            if (searchInput) searchInput.value = initialQ;
            const initialScope = initialParams.get('scope');
            const initialType = initialParams.get('type');
            const initialChat = initialParams.get('chat_id');
            if (scopeSelect && initialScope) scopeSelect.value = initialScope;
            if (typeSelect && initialType) typeSelect.value = initialType;
            if (initialChat) state.currentChannelId = initialChat;
            performSearch();
        }
    }

    behaviors.initSearchHandlers = initSearchHandlers;
})();
