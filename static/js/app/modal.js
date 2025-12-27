(function () {
    const App = window.App = window.App || {};
    const dom = App.dom || {};
    const state = App.state || {};
    const behaviors = App.behaviors = App.behaviors || {};

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
