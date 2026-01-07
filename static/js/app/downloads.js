(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state = App.state || {};
    const behaviors = App.behaviors = App.behaviors || {};

    state.downloadsPoll = state.downloadsPoll || null;

    function formatBytes(num) {
        const n = Number(num || 0);
        if (!isFinite(n) || n <= 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const exp = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
        const value = n / Math.pow(1024, exp);
        return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[exp]}`;
    }

    function formatSpeed(num) {
        const n = Number(num || 0);
        if (!isFinite(n) || n <= 0) return '0 B/s';
        return `${formatBytes(n)}/s`;
    }

    function formatEta(seconds) {
        const n = Number(seconds || 0);
        if (!isFinite(n) || n < 0) return '—';
        const mins = Math.floor(n / 60);
        const secs = Math.floor(n % 60);
        if (mins <= 0) return `${secs}s`;
        return `${mins}m ${secs.toString().padStart(2, '0')}s`;
    }

    async function fetchDownloads() {
        try {
            const res = await fetch('/api/downloads/status');
            if (!res.ok) return [];
            return await res.json();
        } catch (e) {
            console.warn('No se pudo obtener descargas', e);
            return [];
        }
    }

    function renderDownloads(list) {
        if (!dom.downloadsContent) return;
        if (!Array.isArray(list) || list.length === 0) {
            dom.downloadsContent.innerHTML = '<div class="downloads-empty">Sin descargas por ahora.</div>';
            return;
        }
        const rows = list.map(item => {
            const status = item.status || 'unknown';
            const current = Number(item.current || 0);
            const total = Number(item.total || 0);
            const percent = total > 0 ? Math.min(100, Math.round((current / total) * 100)) : 0;
            const speed = formatSpeed(item.speed);
            const eta = status === 'downloading' ? formatEta(item.eta) : (status === 'completed' ? 'Listo' : '—');
            const title = item.filename || item.video_id || 'video';
            const meta = `${formatBytes(current)} / ${formatBytes(total || current)}`;

            return `
                <div class="download-row download-status-${status}">
                    <div class="download-row-main">
                        <div class="download-title" title="${title}">${title}</div>
                        <div class="download-meta">
                            <span class="pill pill-${status}">${status}</span>
                            <span>${meta}</span>
                            <span>${speed}</span>
                            <span>${eta}</span>
                        </div>
                    </div>
                    <div class="download-progress">
                        <div class="download-progress-bar" style="width:${percent}%;"></div>
                    </div>
                </div>
            `;
        }).join('');
        dom.downloadsContent.innerHTML = rows;
    }

    function startDownloadsPolling() {
        if (state.downloadsPoll) return;
        const tick = async () => {
            const data = await fetchDownloads();
            renderDownloads(data);
        };
        tick();
        state.downloadsPoll = setInterval(tick, 2000);
    }

    function stopDownloadsPolling() {
        if (state.downloadsPoll) {
            clearInterval(state.downloadsPoll);
            state.downloadsPoll = null;
        }
    }

    behaviors.openDownloadsModal = function () {
        if (!dom.downloadsOverlay) return;
        dom.downloadsOverlay.classList.add('is-open');
        startDownloadsPolling();
    };

    behaviors.closeDownloadsModal = function () {
        if (!dom.downloadsOverlay) return;
        dom.downloadsOverlay.classList.remove('is-open');
        stopDownloadsPolling();
    };

    // Delegación para cerrar con click en overlay/botón
    if (dom.downloadsOverlay) {
        dom.downloadsOverlay.addEventListener('click', function (ev) {
            if (ev.target === dom.downloadsOverlay) behaviors.closeDownloadsModal?.();
        });
    }
    if (dom.downloadsClose) dom.downloadsClose.addEventListener('click', behaviors.closeDownloadsModal);

})();
