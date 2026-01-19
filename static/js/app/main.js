(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const behaviors = App.behaviors = App.behaviors || {};

    // --- Eventos Globales (Modales y Teclas) ---
    function bindGlobalEvents() {
        // Cierres de modales
        const closeHandlers = [
            { overlay: dom.modalOverlay, close: dom.modalClose, action: behaviors.closeVideoModal },
            { overlay: dom.statsOverlay, close: dom.statsClose, action: behaviors.closeStatsModal },
            { overlay: dom.downloadsOverlay, close: dom.downloadsClose, action: behaviors.closeDownloadsModal }
        ];

        closeHandlers.forEach(({ overlay, close, action }) => {
            if (overlay) overlay.addEventListener('click', (ev) => { if (ev.target === overlay) action?.(); });
            if (close) close.addEventListener('click', action);
        });

        // Botones de apertura
        if (dom.statsButton) dom.statsButton.addEventListener('click', behaviors.openStatsModal);
        if (dom.downloadsButton) dom.downloadsButton.addEventListener('click', behaviors.openDownloadsModal);

        // Tecla Escape
        document.addEventListener('keydown', (event) => {
            if (event.key !== 'Escape') return;
            if (dom.statsOverlay?.classList.contains('is-open')) behaviors.closeStatsModal?.();
            else if (dom.downloadsOverlay?.classList.contains('is-open')) behaviors.closeDownloadsModal?.();
            else if (dom.modalOverlay?.classList.contains('is-open')) behaviors.closeVideoModal?.();
        });

        // Botón Ocultar Video
        if (dom.hideVideoButton) {
            dom.hideVideoButton.addEventListener('click', behaviors.toggleHideCurrentVideo);
        }
        
        // Botón Sync Diario
        const btnSync = document.getElementById("btn-sync-diario");
        if (btnSync) {
            btnSync.addEventListener("click", async () => {
                btnSync.disabled = true;
                btnSync.textContent = "Sincronizando...";
                try {
                    await fetch("/sync/diario", { method: "POST" });
                    alert("Sincronización completada");
                } catch (e) { alert("Error al sincronizar"); } 
                finally {
                    btnSync.disabled = false;
                    btnSync.textContent = "Sincronizar diario";
                }
            });
        }
    }

    // --- Inicialización ---
    function init() {
        // 1. Configurar efectos visuales
        if (behaviors.setupHoverPreviews) behaviors.setupHoverPreviews();
        if (behaviors.setupLazyChatPhotos) behaviors.setupLazyChatPhotos();
        
        // 2. Conectar red (Network)
        if (behaviors.setupFolderConnection) behaviors.setupFolderConnection();
        
        // 3. Bindear eventos
        bindGlobalEvents();
        if (behaviors.initSearchHandlers) behaviors.initSearchHandlers();
        
        // 4. Aplicar filtros iniciales
        if (behaviors.applyHiddenStateToAll) behaviors.applyHiddenStateToAll();
        if (behaviors.applyDuplicateFilter) behaviors.applyDuplicateFilter();
        if (behaviors.applyWatchLaterStateToAll) behaviors.applyWatchLaterStateToAll();
    }

    // Arrancar cuando el DOM esté listo
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();