(() => {
  if (window.ragUsage) return;
  const screens = new Set(['search', 'explorer', 'shifts', 'jobs', 'index', 'settings', 'stats']);
  const id = () => Array.from(crypto.getRandomValues(new Uint8Array(16)), n => n.toString(16).padStart(2, '0')).join('');
  const session = id();
  let enabled = false;
  let screen = '';
  let pending = [];
  let sending = false;
  let firstPage = true;
  let lastRoute = '';
  let navigation = null;
  const push = (kind, action, duration_ms = null, ok = true) => {
    if (!enabled || !screens.has(screen)) return;
    pending.push({event_id: id(), session_id: session, screen, kind, action, duration_ms, ok, _at: performance.now()});
    if (pending.length > 200) pending.shift();
  };
  const flush = async (beacon = false) => {
    if (!enabled || !pending.length || sending) return;
    pending = pending.filter(event => performance.now() - event._at <= 300000);
    const batch = pending.splice(0, 50);
    if (!batch.length) return;
    const body = JSON.stringify({events: batch.map(({_at, ...event}) => ({...event, age_ms: performance.now() - _at}))});
    if (beacon && navigator.sendBeacon?.('/api/usage/events', new Blob([body], {type: 'application/json'}))) return;
    sending = true;
    try {
      const response = await fetch('/api/usage/events', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body,
        credentials: 'same-origin', keepalive: true,
      });
      if (response.status === 401 || response.status === 403) {
        enabled = false;
        pending = [];
      } else if (response.ok) {
        const result = await response.json();
        if (!result.enabled) { enabled = false; pending = []; }
      } else if (response.status !== 429 && response.status >= 500) {
        pending = [...batch, ...pending].slice(-200);
      }
    } catch (_) { pending = [...batch, ...pending].slice(-200); }
    finally { sending = false; }
  };
  // Only static action codes leave the browser, never input values, URLs or file names.
  const attributes = {
    'data-rag-download': 'files.download', 'data-rag-share': 'files.share',
    'data-rag-copy': 'files.copy', 'data-rag-cut': 'files.cut', 'data-rag-delete': 'files.delete',
    'data-rag-archive': 'files.archive', 'data-rag-hide': 'files.hide', 'data-rag-unhide': 'files.unhide',
    'data-rag-favorite-button': 'files.favorite', 'data-rag-refresh-screen': 'control.refresh',
    'data-rag-open-settings': 'navigation.settings',
  };
  const icons = {
    search: 'search.submit', download: 'files.download', upload: 'files.upload', upload_file: 'files.upload',
    share: 'files.share', link: 'files.share', history: 'files.versions', delete: 'files.delete',
    drive_file_rename_outline: 'files.rename', drive_file_move: 'files.move', visibility: 'files.open_file',
    refresh: 'control.refresh', sync: 'control.refresh', more_vert: 'control.more', close: 'control.close',
    save: 'control.save', add: 'control.create', create_new_folder: 'control.create', edit: 'control.edit',
    filter_list: 'control.filter', help_outline: 'control.help', settings: 'navigation.settings',
    light_mode: 'control.theme', dark_mode: 'control.theme', logout: 'control.logout',
    expand_more: 'control.expand', chevron_left: 'control.previous', chevron_right: 'control.next',
    check: 'control.approve', check_circle: 'control.approve', cancel: 'control.cancel', menu: 'control.menu',
  };
  const fixedLabels = {
    'Сохранить': 'control.save', 'Отмена': 'control.cancel', 'Удалить': 'files.delete',
    'Создать': 'control.create', 'Поделиться': 'files.share', 'Переименовать': 'files.rename',
    'Переместить в…': 'files.move', 'Утвердить': 'control.approve', 'На доработку': 'control.reject',
    'Скачать приложение': 'client.download',
  };
  const actionFor = (target) => {
    if (target.dataset.ragUsage) return target.dataset.ragUsage;
    for (const [attribute, action] of Object.entries(attributes)) {
      if (target.hasAttribute(attribute)) return action;
    }
    if (target.hasAttribute('data-rag-open')) {
      return target.closest('[data-rag-type]')?.dataset.ragType === 'folder' ? 'files.open_folder' : 'files.open_file';
    }
    if (target.matches('[role="tab"]')) return 'control.tab';
    if (target.matches('[role="checkbox"], [role="switch"]')) return 'control.checkbox';
    const label = (target.querySelector('.q-btn__content .block, .q-item__label') || target).textContent?.trim();
    if (fixedLabels[label]) return fixedLabels[label];
    const icon = target.querySelector('.q-icon')?.textContent?.trim();
    if (screen === 'shifts') {
      const action = {download: 'export', history: 'history', delete: 'delete', check: 'approve',
        send: 'submit', undo: 'revert', edit: 'edit', save: 'save', manage_accounts: 'manage'}[icon];
      if (action) return `shifts.${action}`;
    }
    return icons[icon] || 'control.button';
  };
  document.addEventListener('click', event => {
    const target = event.target.closest?.('button, a[href], [role="button"], [role="tab"], [role="menuitem"], [role="checkbox"], [role="switch"]');
    if (!target || target.disabled || target.getAttribute('aria-disabled') === 'true') return;
    if (!event.isTrusted) return;
    const action = actionFor(target);
    push('click', action);
    if (action.startsWith('navigation.')) navigation = {screen: action.slice(11), at: performance.now()};
  }, true);
  document.addEventListener('keyup', event => {
    if (event.isTrusted && event.key === 'Enter' && event.target.closest?.('.rag-search-box')) {
      push('click', 'search.submit');
    }
  }, true);
  window.addEventListener('error', () => push('error', 'browser.javascript', null, false));
  window.addEventListener('unhandledrejection', () => push('error', 'browser.promise', null, false));
  let socketBound = false;
  const bindSocket = () => {
    if (socketBound || !window.socket?.on) return;
    socketBound = true;
    window.socket.on('disconnect', () => push('error', 'connection.lost', null, false));
  };
  try {
    const observer = new PerformanceObserver(list => {
      // Event Timing measures input-to-next-paint, not asynchronous download completion.
      for (const entry of list.getEntries()) {
        if (entry.name === 'click' && entry.interactionId) push('timing', 'interaction.response', entry.duration);
      }
    });
    observer.observe({type: 'event', buffered: true, durationThreshold: 16});
  } catch (_) {}
  try {
    const previews = new PerformanceObserver(list => {
      for (const entry of list.getEntries()) {
        const url = new URL(entry.name, location.origin);
        if (url.origin === location.origin && ['/api/view-file', '/api/cloud-drive/preview'].includes(url.pathname)) {
          push('timing', 'files.preview_response', entry.duration, !entry.responseStatus || entry.responseStatus < 400);
        }
      }
    });
    previews.observe({type: 'resource', buffered: false});
  } catch (_) {}
  window.ragUsage = {
    ready(nextScreen, active) {
      const wasEnabled = enabled;
      enabled = Boolean(active);
      if (!enabled) { pending = []; screen = ''; lastRoute = ''; firstPage = false; navigation = null; return; }
      screen = nextScreen;
      if (navigation?.screen === screen) {
        const started = navigation.at;
        const targetScreen = screen;
        navigation = null;
        requestAnimationFrame(() => requestAnimationFrame(() => {
          if (screen === targetScreen && performance.now() - started < 60000) {
            push('timing', 'page.transition', performance.now() - started);
          }
        }));
      }
      if (lastRoute !== screen || !wasEnabled) {
        lastRoute = screen;
        push('page_view', 'page.open');
      }
      if (firstPage) {
        firstPage = false;
        requestAnimationFrame(() => requestAnimationFrame(() => {
          push('timing', 'page.ready', performance.now());
          const nav = performance.getEntriesByType('navigation')[0];
          if (nav) push('timing', 'page.response', nav.responseEnd - nav.startTime);
        }));
      }
      bindSocket();
    },
  };
  setInterval(() => { bindSocket(); flush(); }, 5000);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') flush(true);
  });
  window.addEventListener('pagehide', () => flush(true));
})();
