// Script para iniciar automáticamente el prefetch de videos en segundo plano
// cuando se carga una página de canal

document.addEventListener('DOMContentLoaded', function() {
    // Esperar un momento para asegurar que la página esté completamente cargada
    setTimeout(function() {
        // Detectar si estamos en una vista de canal
        var viewType = document.body.getAttribute('data-view-type');
        if (viewType !== 'files') return;
        
        // Buscar el chat_id en los elementos de video
        var chatId = null;
        var videoItems = document.querySelectorAll('.file-item[data-item-type="video"]');
        if (videoItems.length > 0) {
            chatId = videoItems[0].dataset.chatId;
        }
        
        // Si no hay videos o no tienen chat_id, intentar con la URL
        if (!chatId) {
            var path = window.location.pathname;
            var matches = path.match(/\/channel\/(-?\d+)/);
            if (matches && matches[1]) {
                chatId = matches[1];
            }
        }
        
        // Si encontramos un chat_id, lanzar el prefetch
        if (chatId) {
            console.log('🚀 Iniciando prefetch automático para chat_id:', chatId);
            
            // Mostrar indicador visual
            var statusDiv = document.createElement('div');
            statusDiv.id = 'prefetch-status';
            statusDiv.style.position = 'fixed';
            statusDiv.style.bottom = '20px';
            statusDiv.style.right = '20px';
            statusDiv.style.background = 'rgba(0,0,0,0.7)';
            statusDiv.style.color = '#fff';
            statusDiv.style.padding = '8px 12px';
            statusDiv.style.borderRadius = '4px';
            statusDiv.style.fontSize = '14px';
            statusDiv.style.zIndex = '1000';
            statusDiv.innerHTML = '<i class="fas fa-sync fa-spin"></i> Cargando videos en segundo plano...';
            document.body.appendChild(statusDiv);
            
            // Llamar al endpoint de prefetch
            fetch('/api/prefetch/' + encodeURIComponent(chatId), {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                console.log('✅ Prefetch iniciado:', data);
                // Actualizar indicador visual
                setTimeout(() => {
                    statusDiv.innerHTML = '<i class="fas fa-check"></i> Videos cargados en segundo plano';
                    statusDiv.style.background = 'rgba(40,167,69,0.7)';
                    // Ocultar después de 3 segundos
                    setTimeout(() => {
                        statusDiv.style.opacity = '0';
                        statusDiv.style.transition = 'opacity 0.5s ease';
                        // Eliminar del DOM después de la transición
                        setTimeout(() => statusDiv.remove(), 500);
                    }, 3000);
                }, 1000);
            })
            .catch(error => {
                console.error('❌ Error en prefetch:', error);
                statusDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Error al cargar videos';
                statusDiv.style.background = 'rgba(220,53,69,0.7)';
                setTimeout(() => statusDiv.remove(), 3000);
            });
        }
    }, 500);
});
