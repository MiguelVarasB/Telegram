(function () {
    const g = window.MegaApp = window.MegaApp || { state: {}, helpers: {} };
    const state = g.state;
    const helpers = g.helpers;

    function escapeHtml(str) {
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

    function saveHiddenVideos(set) {
        try {
            localStorage.setItem('hidden_videos', JSON.stringify(Array.from(set || [])));
        } catch (e) {}
    }

    state.hiddenVideos = state.hiddenVideos || loadHiddenVideos();

    helpers.escapeHtml = escapeHtml;
    helpers.escapeHtmlStats = escapeHtml; // alias
    helpers.formatThousands = formatThousands;
    helpers.getHiddenVideos = function () { return state.hiddenVideos || new Set(); };
    helpers.setHiddenVideos = function (set) { state.hiddenVideos = set || new Set(); };
    helpers.saveHiddenVideos = function () { saveHiddenVideos(state.hiddenVideos || new Set()); };
})();
