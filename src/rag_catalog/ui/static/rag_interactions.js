(() => {
  let busyTimer = null;
  const busy = () => document.getElementById('rag-global-busy');

  window.ragShowBusy = (label = 'Выполняется...', options = {}) => {
    const el = busy();
    if (!el) return;
    const text = el.querySelector('.rag-busy-label');
    if (text) text.textContent = String(label || 'Выполняется...');
    el.classList.add('show');
    if (busyTimer) window.clearTimeout(busyTimer);
    const timeout = Number(options.timeout || 1800);
    if (timeout > 0) {
      busyTimer = window.setTimeout(() => window.ragHideBusy(), timeout);
    }
  };

  window.ragHideBusy = () => {
    const el = busy();
    if (!el) return;
    if (busyTimer) window.clearTimeout(busyTimer);
    busyTimer = null;
    el.classList.remove('show');
  };

  const clickBusy = (event) => {
    const target = event.target.closest('button, .q-btn, [role="button"], a[href]');
    if (!target) return;
    if (target.closest('.q-menu, .rag-context-menu')) return;
    if (target.disabled || target.getAttribute('aria-disabled') === 'true') return;
    const raw = target.innerText || target.getAttribute('aria-label') || target.getAttribute('title') || '';
    const label = raw.trim().replace(/\s+/g, ' ').slice(0, 64) || 'Выполняется...';
    window.ragShowBusy(label, { timeout: 1200 });
  };

  if (!window.__ragBusyInstalled) {
    window.__ragBusyInstalled = true;
    document.addEventListener('click', clickBusy, true);
  }

  if (window.__ragContextMenuInstalled) return;
  window.__ragContextMenuInstalled = true;

  const menu = () => document.getElementById('rag-global-context-menu');
  const hide = () => { const m = menu(); if (m) m.style.display = 'none'; };
  const addButton = (m, label, action) => {
    const b = document.createElement('button');
    b.textContent = label;
    b.onclick = () => { hide(); action(); };
    m.appendChild(b);
  };
  const show = (event) => {
    const root = event.target.closest('.q-layout');
    if (!root) return;
    event.preventDefault();
    const m = menu();
    if (!m) return;
    const item = event.target.closest('[data-rag-context="explorer-item"]');
    m.innerHTML = '';
    if (item) {
      const itemType = item.dataset.ragType || 'file';
      const itemPath = decodeURIComponent(item.dataset.ragPath || '');
      const itemUrl = item.dataset.ragUrl || '';
      addButton(m, 'Открыть', () => {
        item.querySelector('[data-rag-open]')?.click();
      });
      if (itemType === 'file' && itemUrl) {
        addButton(m, 'Скачать', () => {
          const downloadButton = item.querySelector('[data-rag-download]');
          if (downloadButton) {
            downloadButton.click();
          } else {
            const a = document.createElement('a');
            a.href = itemUrl;
            a.download = '';
            document.body.appendChild(a);
            a.click();
            a.remove();
          }
        });
      }
      addButton(m, 'Копировать', () => item.querySelector('[data-rag-copy]')?.click());
      addButton(m, 'Вырезать', () => item.querySelector('[data-rag-cut]')?.click());
      addButton(m, 'Удалить', () => item.querySelector('[data-rag-delete]')?.click());
      addButton(m, 'Поделиться', () => item.querySelector('[data-rag-share]')?.click());
      addButton(m, 'Отправить', () => item.querySelector('[data-rag-send]')?.click());
      addButton(m, 'Архивировать', () => item.querySelector('[data-rag-archive]')?.click());
      if (item.dataset.ragHidden === 'true') {
        addButton(m, 'Показать в интерфейсе', () => item.querySelector('[data-rag-unhide]')?.click());
      } else {
        addButton(m, 'Скрыть из интерфейса', () => item.querySelector('[data-rag-hide]')?.click());
      }
      addButton(m, 'Показать в ОС', () => item.querySelector('[data-rag-os]')?.click());
      addButton(m, item.dataset.ragFavorite === 'true' ? 'Убрать из избранного' : 'Добавить в избранное', () => item.querySelector('[data-rag-favorite-button]')?.click());
      addButton(m, 'Поделиться путем', () => navigator.clipboard && navigator.clipboard.writeText(itemPath));
    } else {
      addButton(m, 'Обновить экран', () => location.reload());
      addButton(m, 'Скопировать адрес экрана', () => navigator.clipboard && navigator.clipboard.writeText(location.href));
      addButton(m, 'Настройки', () => { location.href = '/settings'; });
    }
    m.style.left = Math.min(event.clientX, window.innerWidth - 240) + 'px';
    m.style.top = Math.min(event.clientY, window.innerHeight - 160) + 'px';
    m.style.display = 'block';
  };

  let longPressTimer = null;
  let longPressX = 0;
  let longPressY = 0;
  const clearLongPress = () => {
    if (longPressTimer) clearTimeout(longPressTimer);
    longPressTimer = null;
  };
  const longPressSelect = (event) => {
    if (event.pointerType !== 'touch' && event.pointerType !== 'pen') return;
    if (event.target.closest('.rag-select-checkbox, input, textarea, select, .q-menu')) return;
    const item = event.target.closest('.rag-file-table-row, .rag-explorer-item');
    if (!item) return;
    const checkbox = item.querySelector('.rag-select-checkbox');
    if (!checkbox) return;
    longPressX = event.clientX;
    longPressY = event.clientY;
    longPressTimer = window.setTimeout(() => {
      item.dataset.ragLongPress = '1';
      checkbox.click();
      if (navigator.vibrate) navigator.vibrate(12);
      clearLongPress();
    }, 520);
  };
  const longPressMove = (event) => {
    if (!longPressTimer) return;
    if (Math.abs(event.clientX - longPressX) > 10 || Math.abs(event.clientY - longPressY) > 10) {
      clearLongPress();
    }
  };
  const suppressLongPressClick = (event) => {
    const item = event.target.closest('.rag-file-table-row, .rag-explorer-item');
    if (!item || item.dataset.ragLongPress !== '1') return;
    event.preventDefault();
    event.stopPropagation();
    delete item.dataset.ragLongPress;
  };

  document.addEventListener('contextmenu', show);
  document.addEventListener('pointerdown', longPressSelect, { passive: true });
  document.addEventListener('pointermove', longPressMove, { passive: true });
  document.addEventListener('pointerup', clearLongPress, true);
  document.addEventListener('pointercancel', clearLongPress, true);
  document.addEventListener('click', suppressLongPressClick, true);
  document.addEventListener('click', hide);
  document.addEventListener('scroll', hide, true);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });
})();
