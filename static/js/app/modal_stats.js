(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const helpers = App.helpers || {};
    const behaviors = App.behaviors = App.behaviors || {};

    // Cierra el modal de estadísticas y limpia contenido
    function closeStatsModal() {
        if (!dom.statsOverlay) return;
        dom.statsOverlay.classList.remove('is-open');
        if (dom.statsContent) dom.statsContent.innerHTML = '';
    }

    // Obtiene estadísticas desde /api/stats y renderiza tarjetas/listas
    async function loadStats() {
        if (!dom.statsContent) return;
        dom.statsContent.textContent = 'Cargando estadísticas...';
        try {
            const res = await fetch('/api/stats?limit=20');
            if (!res.ok) {
                dom.statsContent.textContent = 'Error al cargar estadísticas';
                return;
            }
            const data = await res.json();

            const total = data.total_videos || 0;
            const unique = data.unique_videos || 0;
            const sinThumb = data.videos_sin_thumb || 0;
            const verticales = data.videos_verticales || 0;
            const largos = data.videos_largos_mas_1h || 0;

            const topGroups = Array.isArray(data.top_groups) ? data.top_groups : [];
            const topNoThumb = Array.isArray(data.top_no_thumb_groups) ? data.top_no_thumb_groups : [];
            const restricted = Array.isArray(data.restricted_forward_groups) ? data.restricted_forward_groups : [];

            let html = '';

            html += '<div class="stats-grid">'               
                + '<div class="stats-card"><div class="stats-label">Videos únicos</div><div class="stats-value">' + helpers.escapeHtmlStats(helpers.formatThousands(unique)) + '</div></div>'
                + '<div class="stats-card"><div class="stats-label">Sin thumb</div><div class="stats-value">' + helpers.escapeHtmlStats(helpers.formatThousands(sinThumb)) + '</div></div>'
                + '<div class="stats-card"><div class="stats-label">Verticales</div><div class="stats-value">' + helpers.escapeHtmlStats(helpers.formatThousands(verticales)) + '</div></div>'
                + '<div class="stats-card"><div class="stats-label">Largos (> 1h)</div><div class="stats-value">' + helpers.escapeHtmlStats(helpers.formatThousands(largos)) + '</div></div>'
                + '</div>';

            html += '<div class="stats-section"><div class="stats-section-title">Grupos con más videos (excluyendo DUMP)</div>';
            if (!topGroups.length) {
                html += '<div class="stats-empty">Sin datos</div>';
            } else {
                html += '<div class="stats-list">';
                topGroups.forEach(function (g, idx) {
                    const pos = idx + 1;
                    const name = g.name || g.chat_id;
                    const tgLink = g.telegram_link || '';

                    let left = '<div class="stats-list-left">' + helpers.escapeHtmlStats(pos) + '. ';
                    if (tgLink) {
                        left += '<a href="' + helpers.escapeHtmlStats(tgLink) + '" target="_blank" rel="noopener noreferrer">'
                            + helpers.escapeHtmlStats(name)
                            + '</a>';
                    } else {
                        left += helpers.escapeHtmlStats(name);
                    }
                    left += '</div>';

                    html += '<div class="stats-list-item">'
                        + left
                        + '<div class="stats-list-right">' + helpers.escapeHtmlStats(helpers.formatThousands(g.videos || 0)) + '</div>'
                        + '</div>';
                });
                html += '</div>';
            }
            html += '</div>';

            html += '<div class="stats-section"><div class="stats-section-title">Canales/Grupos con más videos sin thumb (excluyendo DUMP)</div>';
            if (!topNoThumb.length) {
                html += '<div class="stats-empty">Sin datos</div>';
            } else {
                html += '<div class="stats-list">';
                topNoThumb.forEach(function (g, idx) {
                    const pos = idx + 1;
                    const name = g.name || g.chat_id;
                    const tgLink = g.telegram_link || '';

                    let left = '<div class="stats-list-left">' + helpers.escapeHtmlStats(pos) + '. ';
                    if (tgLink) {
                        left += '<a href="' + helpers.escapeHtmlStats(tgLink) + '" target="_blank" rel="noopener noreferrer">'
                            + helpers.escapeHtmlStats(name)
                            + '</a>';
                    } else {
                        left += helpers.escapeHtmlStats(name);
                    }
                    left += '</div>';

                    const sinThumbFmt = helpers.escapeHtmlStats(helpers.formatThousands(g.sin_thumb || 0));
                    const dumpPct = g.dump_percentage !== undefined && g.dump_percentage !== null
                        ? (' (' + helpers.escapeHtmlStats(g.dump_percentage) + '% DUMP)')
                        : '';

                    html += '<div class="stats-list-item">'
                        + left
                        + '<div class="stats-list-right">' + sinThumbFmt + dumpPct + '</div>'
                        + '</div>';
                });
                html += '</div>';
            }
            html += '</div>';

            html += '<div class="stats-section"><div class="stats-section-title">Grupos que no permiten compartir</div>';
            if (!restricted.length) {
                html += '<div class="stats-empty">Sin datos</div>';
            } else {
                html += '<div class="stats-list">';
                restricted.forEach(function (g, idx) {
                    const pos = idx + 1;
                    const name = g.name || g.chat_id;
                    const tgLink = g.telegram_link || '';

                    let left = '<div class="stats-list-left">' + helpers.escapeHtmlStats(pos) + '. ';
                    if (tgLink) {
                        left += '<a href="' + helpers.escapeHtmlStats(tgLink) + '" target="_blank" rel="noopener noreferrer">'
                            + helpers.escapeHtmlStats(name)
                            + '</a>';
                    } else {
                        left += helpers.escapeHtmlStats(name);
                    }
                    left += '</div>';

                    html += '<div class="stats-list-item">'
                        + left
                        + '<div class="stats-list-right">' + helpers.escapeHtmlStats(helpers.formatThousands(g.blocked || 0)) + '</div>'
                        + '</div>';
                });
                html += '</div>';
            }
            html += '</div>';

            dom.statsContent.innerHTML = html;
        } catch (e) {
            console.error('Error cargando stats', e);
            dom.statsContent.textContent = 'Error al cargar estadísticas';
        }
    }

    // Abre modal de estadísticas y dispara la carga
    function openStatsModal() {
        if (!dom.statsOverlay) return;
        dom.statsOverlay.classList.add('is-open');
        loadStats();
    }

    // Eventos propios del modal de estadísticas
    if (dom.statsButton) dom.statsButton.addEventListener('click', openStatsModal);
    if (dom.statsOverlay) {
        dom.statsOverlay.addEventListener('click', function (ev) {
            if (ev.target === dom.statsOverlay) closeStatsModal();
        });
    }
    if (dom.statsClose) dom.statsClose.addEventListener('click', closeStatsModal);

    behaviors.loadStats = loadStats;
    behaviors.openStatsModal = openStatsModal;
    behaviors.closeStatsModal = closeStatsModal;
})();
