(() => {
  const key = 'rag-telegram-login';
  const endpoint = '/api/auth/telegram/check';
  let pending = sessionStorage.getItem(key);
  let checking = null;
  let timer = null;

  function status(text) {
    const label = document.querySelector('[data-rag-telegram-status]');
    if (label) label.textContent = text;
  }

  function clear() {
    pending = null;
    sessionStorage.removeItem(key);
    clearInterval(timer);
    timer = null;
  }

  function request(method, receipt) {
    return fetch(endpoint, {
      method,
      credentials: 'same-origin',
      cache: 'no-store',
      headers: {'X-Rag-Login-Request': receipt},
      signal: AbortSignal.timeout(10000),
    });
  }

  async function check() {
    const receipt = pending;
    if (!receipt || checking === receipt || document.hidden) return;
    checking = receipt;
    try {
      const response = await request('POST', receipt);
      if (receipt !== pending) return;
      if (response.status === 200) {
        clear();
        location.reload();
      } else if (response.status === 410) {
        clear();
        status('Запрос входа истёк. Закройте это окно и нажмите «Войти через Telegram» ещё раз.');
      }
    } catch {
      // An offline or suspended tab will check again when it returns.
    } finally {
      if (checking === receipt) checking = null;
    }
  }

  function watch() {
    if (!pending) return;
    if (!timer) timer = setInterval(check, 2000);
    check();
  }

  function cancel(receipt) {
    if (!receipt || pending !== receipt) return;
    clear();
    status('');
    request('DELETE', receipt).catch(() => {});
  }

  window.ragTelegramLogin = {
    start(receipt) {
      if (pending !== receipt) cancel(pending);
      pending = receipt;
      sessionStorage.setItem(key, receipt);
      status('После подтверждения в Telegram кабинет откроется в этой вкладке.');
      watch();
    },
    cancel,
  };
  for (const event of ['focus', 'pageshow', 'online']) window.addEventListener(event, check);
  document.addEventListener('visibilitychange', check);
  watch();
})();
