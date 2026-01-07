(function () {
    const form = document.getElementById('tag-form');
    if (!form) return; // Solo en vista de tags

    const keyInput = document.getElementById('tag-key');
    const enInput = document.getElementById('tag-en');
    const esInput = document.getElementById('tag-es');
    const rows = document.getElementById('tags-rows');

    function toast(msg, type = 'info') {
        const el = document.createElement('div');
        el.className = `toast ${type}`;
        el.textContent = msg;
        document.body.appendChild(el);
        requestAnimationFrame(() => el.classList.add('show'));
        setTimeout(() => {
            el.classList.remove('show');
            setTimeout(() => el.remove(), 250);
        }, 2200);
    }

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const key = keyInput.value.trim();
        const name_en = enInput.value.trim();
        const name_es = esInput.value.trim();
        if (!key || !name_en || !name_es) {
            toast('Completa todos los campos', 'error');
            return;
        }
        try {
            const res = await fetch('/api/tags', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ key, name_en, name_es }),
            });
            if (!res.ok) throw new Error(await res.text());
            upsertRow({ key, name_en, name_es });
            form.reset();
            keyInput.focus();
            toast('Guardado');
        } catch (err) {
            console.error(err);
            toast('Error guardando', 'error');
        }
    });

    rows?.addEventListener('click', (e) => {
        const btn = e.target.closest('.edit-tag');
        if (!btn) return;
        const row = btn.closest('.tags-row');
        if (!row) return;
        keyInput.value = row.dataset.key || '';
        enInput.value = row.querySelector('.tag-en')?.textContent || '';
        esInput.value = row.querySelector('.tag-es')?.textContent || '';
        keyInput.focus();
    });

    function upsertRow(tag) {
        let row = rows.querySelector(`.tags-row[data-key="${tag.key}"]`);
        if (!row) {
            row = document.createElement('div');
            row.className = 'tags-row';
            row.dataset.key = tag.key;
            row.innerHTML = `
                <span class="tag-key"></span>
                <span class="tag-en"></span>
                <span class="tag-es"></span>
                <button class="btn small ghost edit-tag" type="button" data-key="${tag.key}">
                    <i class="fas fa-pen"></i>
                </button>
            `;
            rows.appendChild(row);
            rows.querySelectorAll('.empty').forEach((n) => n.remove());
        }
        row.querySelector('.tag-key').textContent = tag.key;
        row.querySelector('.tag-en').textContent = tag.name_en;
        row.querySelector('.tag-es').textContent = tag.name_es;
    }
})();
