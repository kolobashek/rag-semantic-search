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

  const installDiagnostics = () => {
    if (window._ragDiagInit) return;
    window._ragDiagInit = true;
    const now = () => Math.round(performance.now());
    const sid = sessionStorage.getItem('rag-diag-session') ||
      (Date.now().toString(36) + '-' + Math.random().toString(36).slice(2));
    sessionStorage.setItem('rag-diag-session', sid);
    const basePayload = () => {
      const text = document.body ? document.body.innerText : '';
      const input = document.querySelector('.rag-search-box input, input[placeholder^="Введите название"]');
      return {
        session_id: sid,
        url: location.href,
        path: location.pathname,
        ready_state: document.readyState,
        online: navigator.onLine,
        visibility: document.visibilityState,
        elapsed_ms: now(),
        viewport: {
          width: window.innerWidth,
          height: window.innerHeight,
          dpr: window.devicePixelRatio || 1,
          scroll_y: Math.round(window.scrollY || 0),
        },
        search: {
          query: input ? String(input.value || '').slice(0, 250) : '',
          has_results_header: text.includes('Результаты по запросу'),
          has_loading_label: text.includes('Ищу совпадения'),
          has_connection_lost: text.includes('Соединение потеряно'),
          has_preparing_error: text.includes('Файловый индекс еще подготавливается'),
          has_semantic_timeout: text.includes('Семантический поиск не ответил быстро'),
        },
      };
    };
    const send = (action, details = {}) => {
      const body = JSON.stringify({ action, ...basePayload(), details });
      try {
        if (navigator.sendBeacon) {
          const blob = new Blob([body], { type: 'application/json' });
          if (navigator.sendBeacon('/api/ui-events', blob)) return;
        }
      } catch (_) {}
      fetch('/api/ui-events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        keepalive: true,
      }).catch(() => {});
    };
    window.ragDiagLog = send;

    const navEntry = performance.getEntriesByType && performance.getEntriesByType('navigation')[0];
    send('client_diag_ready', { navigation_type: navEntry ? navEntry.type : '' });
    window.addEventListener('online', () => send('browser_online'));
    window.addEventListener('offline', () => send('browser_offline'));
    window.addEventListener('visibilitychange', () => send('visibility_change'));
    window.addEventListener('pagehide', (event) => send('pagehide', { persisted: !!event.persisted }));
    window.addEventListener('pageshow', (event) => send('pageshow', { persisted: !!event.persisted }));
    window.addEventListener('beforeunload', () => send('beforeunload'));
    window.addEventListener('error', (event) => send('javascript_error', {
      message: String(event.message || '').slice(0, 1000),
      source: String(event.filename || '').slice(0, 500),
      line: event.lineno || 0,
      col: event.colno || 0,
    }));
    window.addEventListener('unhandledrejection', (event) => send('unhandled_rejection_error', {
      reason: String(event.reason && (event.reason.stack || event.reason.message || event.reason) || '').slice(0, 1500),
    }));

    const wrapHistory = (name) => {
      const original = history[name];
      if (!original || original._ragDiagWrapped) return;
      const wrapped = function(...args) {
        const result = original.apply(this, args);
        setTimeout(() => send('route_change', { method: name }), 0);
        return result;
      };
      wrapped._ragDiagWrapped = true;
      history[name] = wrapped;
    };
    wrapHistory('pushState');
    wrapHistory('replaceState');
    window.addEventListener('popstate', () => send('route_change', { method: 'popstate' }));

    let lastLost = false;
    let lastLoading = false;
    let lastPreparing = false;
    const scan = () => {
      const text = document.body ? document.body.innerText : '';
      const lost = text.includes('Соединение потеряно');
      const loading = text.includes('Ищу совпадения');
      const preparing = text.includes('Файловый индекс еще подготавливается');
      if (lost !== lastLost) {
        lastLost = lost;
        send(lost ? 'connection_lost_visible' : 'connection_lost_hidden', { excerpt: text.slice(0, 1200) });
      }
      if (loading !== lastLoading) {
        lastLoading = loading;
        send(loading ? 'search_loading_visible' : 'search_loading_hidden');
      }
      if (preparing !== lastPreparing) {
        lastPreparing = preparing;
        send(preparing ? 'preparing_error_visible' : 'preparing_error_hidden', { excerpt: text.slice(0, 1200) });
      }
    };
    let pending = false;
    const scheduleScan = () => {
      if (pending) return;
      pending = true;
      setTimeout(() => {
        pending = false;
        scan();
      }, 120);
    };
    new MutationObserver(scheduleScan).observe(document.documentElement, {
      childList: true,
      subtree: true,
      characterData: true,
    });
    setInterval(scan, 2000);
    scan();
  };
  installDiagnostics();

  const installOverflowTitles = () => {
    if (window.__ragOverflowTitlesInstalled) return;
    window.__ragOverflowTitlesInstalled = true;

    const textBoxFor = (el) => el.querySelector('.block') || el;
    const isOverflowing = (el) => el && el.scrollWidth > el.clientWidth + 1;
    const sync = (event) => {
      const el = event.target.closest('[data-rag-overflow-title]');
      if (!el) return;
      const textBox = textBoxFor(el);
      if (isOverflowing(textBox)) {
        el.setAttribute('title', el.dataset.ragOverflowTitle || '');
      } else {
        el.removeAttribute('title');
      }
    };
    const clear = (event) => {
      const el = event.target.closest('[data-rag-overflow-title]');
      if (el) el.removeAttribute('title');
    };

    document.addEventListener('mouseenter', sync, true);
    document.addEventListener('focusin', sync, true);
    document.addEventListener('mouseleave', clear, true);
    document.addEventListener('focusout', clear, true);
  };
  installOverflowTitles();

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
