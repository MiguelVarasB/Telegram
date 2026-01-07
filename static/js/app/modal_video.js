(function () {
    const App = window.App = window.App || {};
    const dom = App.dom = App.dom || {};
    const state = App.state = App.state || {};
    const behaviors = App.behaviors = App.behaviors || {};

    function ensureDom() {
        dom.modalOverlay = dom.modalOverlay || document.getElementById('video-modal-overlay');
        dom.modalVideo = dom.modalVideo || document.getElementById('modal-video');
        dom.modalClose = dom.modalClose || document.getElementById('modal-close-btn');
    }

    // Delegación para abrir modal al hacer click en el thumbnail
    document.addEventListener('click', function (event) {
        const link = event.target.closest('.video-thumb-link');
        if (!link) return;
        const card = link.closest('.file-item[data-item-type="video"]');
        const url = card?.dataset.streamUrl || link.getAttribute('href');
        if (!url) return;
        event.preventDefault();
        event.stopPropagation();
        ensureDom();
        behaviors.openVideoModal?.(url, card);
    });

    function bindCloseEvents() {
        ensureDom();
        if (dom.modalOverlay && !dom.modalOverlay._videoCloseBound) {
            dom.modalOverlay.addEventListener('click', function (ev) {
                if (ev.target === dom.modalOverlay) behaviors.closeVideoModal?.();
            });
            dom.modalOverlay._videoCloseBound = true;
        }
        if (dom.modalClose && !dom.modalClose._videoCloseBound) {
            dom.modalClose.addEventListener('click', behaviors.closeVideoModal);
            dom.modalClose._videoCloseBound = true;
        }
    }

    // Cierre al clickear overlay/botón (esperamos al DOM)
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindCloseEvents);
    } else {
        bindCloseEvents();
    }

    // Escape para cerrar el modal de video
    document.addEventListener('keydown', function (event) {
        if (event.key !== 'Escape') return;
        ensureDom();
        if (dom.modalOverlay && dom.modalOverlay.classList.contains('is-open')) {
            behaviors.closeVideoModal?.();
        }
    });

    // Abre modal de video y sincroniza estados/botones
    function openVideoModal(url, sourceEl) {
        ensureDom();
        if (!dom.modalOverlay || !dom.modalVideo || !url) return;
        dom.modalVideo.src = url;
        dom.modalVideo.load();

        if (sourceEl) {
            state.currentVideoElement = sourceEl;
            state.currentVideoId = sourceEl.dataset.videoId || null;
            behaviors.fillVideoDetailsFromElement?.(sourceEl);
            behaviors.loadVideoMessages?.(state.currentVideoId);
            behaviors.updateHideVideoButton?.();
            behaviors.updateWatchLaterButton?.();
        } else {
            state.currentVideoElement = null;
            state.currentVideoId = null;
            behaviors.updateHideVideoButton?.();
            behaviors.updateWatchLaterButton?.();
        }

        dom.modalOverlay.classList.add('is-open');
    }

    // Cierra modal de video y limpia estado/preview
    function closeVideoModal() {
        ensureDom();
        if (!dom.modalOverlay || !dom.modalVideo) return;
        dom.modalOverlay.classList.remove('is-open');
        dom.modalVideo.pause();
        dom.modalVideo.removeAttribute('src');
        dom.modalVideo.load();
        behaviors.clearVideoDetails?.();
        behaviors.clearVideoMessages?.();

        if (dom.messagesContainer) dom.messagesContainer.style.display = 'none';
        if (dom.msgToggleIcon) {
            dom.msgToggleIcon.classList.remove('fa-chevron-up');
            dom.msgToggleIcon.classList.add('fa-chevron-down');
        }
    }

    behaviors.openVideoModal = openVideoModal;
    behaviors.closeVideoModal = closeVideoModal;
})();
