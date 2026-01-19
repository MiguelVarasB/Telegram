(function () {
    const App = window.App = window.App || {};
    const state = App.state = App.state || {};
    const behaviors = App.behaviors = App.behaviors || {};

    const btnSyncFaltantes = document.getElementById('btn-sync-faltantes');

    // --- UI del Botón Sincronizar ---
    behaviors.updateBatchButtonUI = function() {
        if (!btnSyncFaltantes) return;
        
        if (!state.batchScan || !state.batchScan.jobId || state.batchScan.running === 0) {
            btnSyncFaltantes.textContent = 'Sincronizar faltantes';
            btnSyncFaltantes.disabled = false;
        } else {
            const { done, total, running } = state.batchScan;
            btnSyncFaltantes.textContent = `Escaneando (${done}/${total}, ${running} activos)`;
            btnSyncFaltantes.disabled = true;
        }
    };

    // --- Acción: Iniciar Sincronización Masiva ---
    behaviors.triggerBatchScan = async function() {
        const container = document.querySelector('.container[data-folder-id]');
        const folderId = container?.getAttribute('data-folder-id') || null;
        
        // Seleccionar chats con videos faltantes
        const cards = Array.from(document.querySelectorAll('.file-item[data-chat-id]'))
            .filter((el) => Number(el.dataset.faltantes || 0) > 0)
            .sort((a, b) => Number(a.dataset.faltantes || 0) - Number(b.dataset.faltantes || 0));

        if (!btnSyncFaltantes) return;
        
        if (!cards.length) {
            btnSyncFaltantes.textContent = 'Nada que sincronizar';
            setTimeout(() => behaviors.updateBatchButtonUI(), 1500);
            return;
        }

        const chatIds = cards.map((el) => Number(el.dataset.chatId)).filter((n) => Number.isFinite(n));
        
        btnSyncFaltantes.textContent = `Enviando (${chatIds.length})...`;
        btnSyncFaltantes.disabled = true;

        try {
            const res = await fetch('/api/folder/scan-batch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ chat_ids: chatIds, folder_id: folderId ? Number(folderId) : null }),
            });
            
            if (!res.ok) throw new Error('Error scan-batch');
            
            const data = await res.json();
            state.batchScan = { 
                jobId: data.job_id, 
                total: data.total || chatIds.length, 
                done: 0, 
                running: 0 
            };
            behaviors.updateBatchButtonUI();
        } catch (e) {
            console.error(e);
            btnSyncFaltantes.textContent = 'Error al iniciar';
            setTimeout(() => behaviors.updateBatchButtonUI(), 2000);
        }
    };

    // --- Acción: Ver Más Tarde (Global) ---
    window.toggleWatchLater = async function(e, itemId) {
        e.preventDefault();
        e.stopPropagation();
        
        const card = e.currentTarget.closest('.file-item');
        const current = card?.dataset.watchLater === '1';
        const nextValue = !current;

        try {   
            const res = await fetch(`/api/video/${encodeURIComponent(itemId)}/watch_later`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ value: nextValue }),
            });
            
            if (!res.ok) throw new Error();
            const data = await res.json();
            
            if (card) {
                card.dataset.watchLater = data.watch_later ? '1' : '0';
                if (behaviors.applyWatchLaterStateToElement) {
                    behaviors.applyWatchLaterStateToElement(card);
                }
            }
        } catch (err) {
            alert('Error al actualizar "ver más tarde".');
        }
    };
    
    // Asignar evento al botón si existe
    if (btnSyncFaltantes) {
        btnSyncFaltantes.addEventListener('click', behaviors.triggerBatchScan);
    }
})();