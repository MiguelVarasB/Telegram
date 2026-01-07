(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state || {};
    const behaviors = App.behaviors = App.behaviors || {};

    // Delegación para abrir modal al hacer click en el thumbnail
    document.addEventListener('click', function (event) {
        const link = event.target.closest('.video-thumb-link');
        if (!link) return;
        const card = link.closest('.file-item[data-item-type="video"]');
        const url = card?.dataset.streamUrl || link.getAttribute('href');
        if (!url) return;
        event.preventDefault();
        event.stopPropagation();
        behaviors.openVideoModal?.(url, card);
    });

    // Cierre al clickear el overlay
    if (dom.modalOverlay) {
        dom.modalOverlay.addEventListener('click', function (ev) {
            if (ev.target === dom.modalOverlay) behaviors.closeVideoModal?.();
        });
    }
    if (dom.modalClose) dom.modalClose.addEventListener('click', behaviors.closeVideoModal);

    // Abre modal de video y sincroniza estados/botones
    function openVideoModal(url, sourceEl) {
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
