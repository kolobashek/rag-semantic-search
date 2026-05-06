/* v2 — addressing Dmitry's feedback. Compact wireframes per topic. */

// Collapsible block helper (visual only)
const Spoiler = ({ x, y, w, title, open = true, count, children, h = 60 }) => (
  <g>
    <SketchBox x={x} y={y} w={w} h={h} rough={0.4} rounded fill="#fff" />
    <SketchLine x1={x} y1={y + 28} x2={x + w} y2={y + 28} stroke="#eee" />
    <Scribble x={x + 16} y={y + 20} size={11} family="Caveat" weight={700}>
      {open ? '▾' : '▸'}  {title}
    </Scribble>
    {count != null && (
      <Scribble x={x + w - 16} y={y + 20} size={10} family="JetBrains Mono" anchor="end" color="#888">
        {count}
      </Scribble>
    )}
    {open && children}
  </g>
);

/* === 01 LOGIN — B layout + A form, "запросить доступ" === */
const V2_Login = () => (
  <svg width="1100" height="640" viewBox="0 0 1100 640">
    <SketchBox x={0} y={0} w={1100} h={640} fill="#fdfcf7" rough={0} />

    {/* Left: brand panel (B style) */}
    <SketchBox x={0} y={0} w={520} h={640} fill="#1d1d1d" rough={0} />
    <Circle cx={56} cy={56} r={14} fill="#fff" />
    <Scribble x={56} y={61} size={11} family="Caveat" weight={700} anchor="middle" color="#1d1d1d">RC</Scribble>
    <Scribble x={80} y={61} size={14} family="Caveat" weight={600} color="#fff">RAG Каталог</Scribble>

    <Scribble x={48} y={260} size={48} family="Caveat" weight={700} color="#fff">Документы.</Scribble>
    <Scribble x={48} y={306} size={48} family="Caveat" weight={700} color="#fff">Найдём всё.</Scribble>
    <Scribble x={48} y={350} size={16} family="Kalam" color="#aaa">Семантический поиск по 35K+ файлов компании.</Scribble>
    <Scribble x={48} y={372} size={16} family="Kalam" color="#aaa">Карточки, договоры, паспорта, переписка — за секунду.</Scribble>

    <Scribble x={48} y={596} size={11} family="Kalam" color="#666">v2.0 · 28.04.26 · поддержка @danofigdolat</Scribble>

    {/* Right: form (A style — компактная форма) */}
    <Scribble x={696} y={148} size={28} family="Caveat" weight={700} anchor="middle">Войти</Scribble>
    <SketchLine x1={650} y1={160} x2={742} y2={160} stroke={inkColor} strokeWidth={2.5} />

    <SketchInput x={580} y={196} w={440} h={42} label="Логин" />
    <SketchInput x={580} y={250} w={440} h={42} label="Пароль" icon="•" />

    <SketchBox x={580} y={306} w={14} h={14} rough={0.2} fill="#fff" />
    <Scribble x={602} y={318} size={11} family="Kalam">запомнить меня</Scribble>
    <Scribble x={1020} y={318} size={11} family="Kalam" anchor="end" color={accentInk} weight={700}>забыли пароль?</Scribble>

    <SketchButton x={580} y={342} w={440} h={46} label="войти →" filled color={inkColor} />

    <SketchLine x1={580} y1={418} x2={760} y2={418} stroke="#ddd" />
    <Scribble x={800} y={422} size={11} family="Kalam" anchor="middle" color="#888">или</Scribble>
    <SketchLine x1={840} y1={418} x2={1020} y2={418} stroke="#ddd" />

    <SketchButton x={580} y={438} w={440} h={42} label="✈ Войти через Telegram" />

    <SketchLine x1={580} y1={502} x2={1020} y2={502} stroke="#eee" />
    <Scribble x={800} y={528} size={13} family="Kalam" anchor="middle" color="#666">Нет учётной записи?</Scribble>
    <SketchButton x={620} y={544} w={360} h={40} label="запросить доступ" />

    <Callout from={[1100, 348]} to={[1024, 366]} label="«запросить доступ» —" side="top" />
    <Scribble x={1100} y={384} size={13} color={accentRed} family="Caveat" weight={600}>не «регистрация»</Scribble>
  </svg>
);

/* === 02 HOME — Statistics + own history. Tasks/calendar/files configurable === */
const V2_Home = () => (
  <svg width="1280" height="900" viewBox="0 0 1280 900">
    <SketchBox x={0} y={0} w={1280} h={900} fill="#f7f4ec" rough={0} />

    {/* Header */}
    <SketchBox x={0} y={0} w={1280} h={56} fill="#1d1d1d" rough={0} />
    <Circle cx={28} cy={28} r={10} fill="#fff" />
    <Scribble x={28} y={33} size={11} family="Caveat" weight={700} anchor="middle" color="#1d1d1d">RC</Scribble>
    <Scribble x={50} y={32} size={14} family="Caveat" color="#fff" weight={600}>RAG Каталог</Scribble>

    {/* collapsible search in header (icon mode) */}
    <Tag x={460} y={18} label="🔍 поиск (⌘K)" color="#fff" w={240} />
    <Scribble x={1220} y={32} size={13} family="Caveat" anchor="end" color="#fff">A admin   ☾</Scribble>

    {/* tabs */}
    <SketchLine x1={0} y1={88} x2={1280} y2={88} stroke="#ddd" />
    {['⌂ Главная','🔍 Поиск','📁 Файлы','⚙ Индекс','📊 Аналитика'].map((t, i) => (
      <g key={i}>
        <Scribble x={48 + i * 110} y={78} size={13} family="Caveat" weight={i === 0 ? 700 : 500} color={i === 0 ? inkColor : '#888'}>{t}</Scribble>
        {i === 0 && <SketchLine x1={32 + i * 110} y1={86} x2={108 + i * 110} y2={86} stroke={inkColor} strokeWidth={2.5} />}
      </g>
    ))}

    {/* Greeting + customize */}
    <Scribble x={32} y={124} size={28} family="Caveat" weight={700}>Доброе утро, admin</Scribble>
    <Scribble x={32} y={146} size={13} family="Kalam" color="#777">28 апреля 2026 · среда</Scribble>
    <Scribble x={1248} y={134} size={12} family="Caveat" anchor="end" color={accentInk} weight={700}>⊞ настроить главную</Scribble>

    {/* Show all / hide all (global accordion control) */}
    <Scribble x={1248} y={172} size={11} family="Kalam" anchor="end" color="#666">⊟ свернуть всё   ⊞ развернуть всё</Scribble>

    {/* === BLOCK 1: General stats (always shown by default) === */}
    <Spoiler x={32} y={186} w={1216} h={148} title="Общая статистика по системе" count="4 показателя">
      {[
        { l: 'Файлов в БД', v: '35 167', s: '+143 за сутки', c: '#16a34a' },
        { l: 'Размер индекса', v: '12.8 ГБ', s: 'из 28 ГБ', c: '#888' },
        { l: 'Поисков сегодня', v: '187', s: 'команда · +23%', c: '#16a34a' },
        { l: 'Активных польз.', v: '4 / 12', s: 'сейчас в системе', c: '#888' },
      ].map((m, i) => (
        <g key={i}>
          <Scribble x={48 + i * 304} y={264} size={10} family="Kalam" color="#888" weight={700}>{m.l.toUpperCase()}</Scribble>
          <Scribble x={48 + i * 304} y={300} size={28} family="Caveat" weight={700}>{m.v}</Scribble>
          <Scribble x={48 + i * 304} y={322} size={11} family="Kalam" color={m.c}>{m.s}</Scribble>
          {i < 3 && <SketchLine x1={336 + i * 304} y1={244} x2={336 + i * 304} y2={326} stroke="#eee" />}
        </g>
      ))}
    </Spoiler>

    {/* === BLOCK 2: My search history (own) === */}
    <Spoiler x={32} y={350} w={600} h={260} title="🕒 Моя история поиска" count="личное · 24 ч">
      {[
        { q: 'карточка предприятия', t: '14:23', n: 50 },
        { q: 'PC300', t: '12:08', n: 16 },
        { q: 'Спецмаш PC300', t: '11:42', n: 14 },
        { q: 'паспорт', t: '10:15', n: 11 },
        { q: 'счёт 4153', t: 'вчера', n: 1 },
        { q: 'доверенность Грищенко', t: 'вчера', n: 3 },
      ].map((h, i) => (
        <g key={i}>
          <SketchLine x1={48} y1={400 + i * 32} x2={616} y2={400 + i * 32} stroke="#eee" />
          <Scribble x={64} y={420 + i * 32} size={11} family="JetBrains Mono" color="#888">{h.t}</Scribble>
          <Scribble x={140} y={420 + i * 32} size={13} family="Caveat" weight={600}>{h.q}</Scribble>
          <Tag x={560} y={408 + i * 32} label={`${h.n}`} color="#666" w={40} />
        </g>
      ))}
    </Spoiler>

    {/* === BLOCK 3: Index status (общая статистика) === */}
    <Spoiler x={648} y={350} w={600} h={260} title="⚙ Состояние индекса" count="общее">
      <Tag x={780} y={368} label="● running · 72%" color={accentInk} filled />
      <Scribble x={664} y={416} size={12} family="Kalam" weight={600}>large этап</Scribble>
      <Scribble x={1232} y={416} size={11} family="JetBrains Mono" anchor="end" color="#888">328 / 11 800</Scribble>
      <SketchBox x={664} y={424} w={568} h={10} rough={0.2} rounded fill="#eee" />
      <SketchBox x={664} y={424} w={409} h={10} rough={0.2} rounded fill={accentInk} />
      {['metadata','small','large','content','OCR'].map((s, i) => (
        <g key={i}>
          <Circle cx={684 + i * 110} cy={462} r={8} fill={i < 2 ? '#16a34a' : i === 2 ? accentInk : '#ddd'} />
          <Scribble x={684 + i * 110} y={466} size={9} family="Caveat" anchor="middle" weight={700} color="#fff">
            {i < 2 ? '✓' : i === 2 ? '◷' : ''}
          </Scribble>
          <Scribble x={684 + i * 110} y={488} size={10} family="Kalam" anchor="middle" color="#666">{s}</Scribble>
        </g>
      ))}
      <SketchLine x1={664} y1={510} x2={1232} y2={510} stroke="#eee" />
      <Scribble x={664} y={530} size={11} family="Kalam" color="#888">~30 мин до завершения · последний полный 7ч 52м · 0 ошибок</Scribble>
      <Scribble x={1232} y={552} size={11} family="Caveat" anchor="end" color={accentInk} weight={700}>детали →</Scribble>
    </Spoiler>

    {/* === BLOCK 4: Customizable widgets === */}
    <Scribble x={32} y={642} size={16} family="Caveat" weight={700}>Виджеты (настраиваемые)</Scribble>
    <Tag x={216} y={632} label="+ добавить виджет" color={accentInk} filled />
    <Scribble x={1248} y={642} size={11} family="Kalam" anchor="end" color="#888">перетаскиванием меняйте порядок ⇄</Scribble>

    {/* Tasks widget */}
    <Spoiler x={32} y={660} w={400} h={210} title="📋 Мои задачи" count="3 активных">
      {[
        { t: 'Проверить small chunks (1408 ошибок)', d: 'сегодня', c: '#dc2626' },
        { t: '38 заявок на доступ ждут', d: 'сегодня', c: '#f59e0b' },
        { t: 'Настроить ночной OCR', d: 'до 30.04', c: '#888' },
      ].map((t, i) => (
        <g key={i}>
          <SketchBox x={48} y={714 + i * 44} w={12} h={12} rough={0.2} fill="#fff" />
          <Scribble x={68} y={724 + i * 44} size={12} family="Kalam">{t.t}</Scribble>
          <Tag x={68} y={732 + i * 44} label={t.d} color={t.c} />
        </g>
      ))}
    </Spoiler>

    {/* Calendar widget */}
    <Spoiler x={448} y={660} w={400} h={210} title="📅 Календарь индексации" count="неделя">
      {[
        { d: 'пн 28', s: '02:00 полный', t: '7:52' },
        { d: 'вт 29', s: '02:00 полный', t: '~7ч' },
        { d: 'ср 30', s: '03:00 инкр + OCR', t: '?' },
      ].map((c, i) => (
        <g key={i}>
          <SketchLine x1={464} y1={730 + i * 38} x2={832} y2={730 + i * 38} stroke="#eee" />
          <Scribble x={480} y={722 + i * 38} size={12} family="JetBrains Mono" color="#666" weight={500}>{c.d}</Scribble>
          <Scribble x={560} y={722 + i * 38} size={12} family="Kalam">{c.s}</Scribble>
          <Scribble x={820} y={722 + i * 38} size={11} family="JetBrains Mono" anchor="end" color="#888">{c.t}</Scribble>
        </g>
      ))}
    </Spoiler>

    {/* Recent files widget */}
    <Spoiler x={864} y={660} w={384} h={210} title="📁 Недавно открытые" count="5 файлов">
      {['Карточка Спецмаш 2026.pdf','Доверенность 15ПП.pdf','ТКО 175813.docx','Реквизиты.pdf','vin lovol.docx'].map((f, i) => (
        <g key={i}>
          <SketchBox x={880} y={712 + i * 28} w={10} h={10} rough={0.2} fill={i === 0 || i === 1 ? '#dc2626' : '#2563eb'} />
          <Scribble x={898} y={722 + i * 28} size={11} family="Caveat" weight={500}>{f}</Scribble>
        </g>
      ))}
    </Spoiler>

    <StickyNote x={32} y={882} w={680} h={14} color="#bfdbfe">
      Все блоки сворачиваются. Порядок и набор виджетов — настраивается. По умолчанию — Статистика + История поиска.
    </StickyNote>
  </svg>
);

/* === 03 SEARCH RESULTS — preview hidden, group/sort, refine, stats === */
const V2_Search = () => (
  <svg width="1280" height="940" viewBox="0 0 1280 940">
    <SketchBox x={0} y={0} w={1280} h={940} fill="#f7f4ec" rough={0} />

    {/* search bar */}
    <SketchBox x={32} y={24} w={1216} h={44} rough={0.4} rounded fill="#fff" stroke={inkColor} />
    <Scribble x={56} y={52} size={15} family="Caveat" color="#1d1d1d" weight={600}>🔍 карточка предприятия</Scribble>
    <Tag x={1180} y={36} label="× очистить" color="#999" />

    {/* Refine search (искать в найденных) */}
    <SketchBox x={32} y={80} w={1216} h={36} rough={0.4} rounded fill={accentYellow} opacity={0.4} stroke="#a89020" />
    <Scribble x={48} y={102} size={11} family="Kalam" color="#666" weight={700}>↳ УТОЧНИТЬ В НАЙДЕННОМ:</Scribble>
    <SketchBox x={216} y={88} w={500} h={20} rough={0.3} rounded fill="#fff" />
    <Scribble x={232} y={102} size={12} family="Caveat" color="#aaa">введите слово для уточнения…</Scribble>
    <Tag x={736} y={88} label="× Спецмаш" color={accentInk} filled />
    <Tag x={820} y={88} label="× 2026" color={accentInk} filled />
    <Scribble x={1232} y={102} size={11} family="Kalam" anchor="end" color="#888">3 уточнения активны</Scribble>

    {/* === Stats card with refining numbers === */}
    <Spoiler x={32} y={128} w={1216} h={120} title="Статистика выдачи" count="50 → 12 после уточнения" h={120}>
      {[
        { l: 'Точные в названиях', v: 8, c: '#16a34a' },
        { l: 'Точные в содержимом', v: 4, c: '#2563eb' },
        { l: 'Семантически близкие', v: 38, c: '#888' },
        { l: 'Всего результатов', v: 50, c: inkColor },
      ].map((m, i) => (
        <g key={i}>
          <Scribble x={48 + i * 200} y={186} size={10} family="Kalam" color="#888" weight={700}>{m.l.toUpperCase()}</Scribble>
          <Scribble x={48 + i * 200} y={216} size={26} family="Caveat" weight={700} color={m.c}>{m.v}</Scribble>
        </g>
      ))}
      {/* file-extension breakdown */}
      <SketchLine x1={840} y1={172} x2={840} y2={236} stroke="#eee" />
      <Scribble x={856} y={186} size={10} family="Kalam" color="#888" weight={700}>ПО РАСШИРЕНИЯМ</Scribble>
      {[
        ['📁','folder', 4],
        ['📕','pdf', 18],
        ['📄','docx', 22],
        ['📊','xlsx', 4],
        ['🖼','jpg', 2],
      ].map((e, i) => (
        <g key={i}>
          <Tag x={856 + i * 78} y={200} label={`${e[1]} · ${e[2]}`} color="#666" w={70} />
        </g>
      ))}
    </Spoiler>

    {/* === Toolbar: sort / group / view / actions === */}
    <SketchBox x={32} y={262} w={1216} h={44} rough={0.4} rounded fill="#fff" />
    <Scribble x={48} y={288} size={12} family="Kalam" weight={600}>50 результатов</Scribble>
    <Scribble x={160} y={288} size={11} family="Kalam" color="#888">сортировка:</Scribble>
    <Tag x={232} y={274} label="релевантность ▾" color={inkColor} />
    <Scribble x={356} y={288} size={11} family="Kalam" color="#888">группировка:</Scribble>
    <Tag x={432} y={274} label="по типу ▾" color={inkColor} />
    <Scribble x={524} y={288} size={11} family="Kalam" color="#888">вид:</Scribble>
    <Tag x={560} y={274} label="☰ список" color={accentInk} filled />
    <Tag x={638} y={274} label="⊞ карточки" color="#888" />
    <Tag x={720} y={274} label="☷ таблица" color="#888" />
    <Tag x={800} y={274} label="⊟ компакт" color="#888" />
    <Scribble x={1232} y={288} size={11} family="Kalam" anchor="end" color={accentInk} weight={700}>⤓ экспорт CSV   ⊞ настроить колонки</Scribble>

    {/* === Result list with hidden preview === */}
    {/* Group: Папки */}
    <Scribble x={32} y={332} size={11} family="Kalam" weight={700} color="#888">▾ ПАПКИ · 3</Scribble>
    {[
      ['Карточки предприятий','Магазин/Карточки предпр.','—','12.04.26 · изм. 12.04.26'],
      ['Карточки предприятий','Магазин/Раб.стол/Карточки','—','11.04.26'],
      ['Карточка предпр., доверенности','Магазин/Нужное/...','—','08.04.26'],
    ].map((row, ri) => {
      const y = 348 + ri * 44;
      return (
        <g key={ri}>
          <SketchBox x={32} y={y} w={1216} h={40} rough={0.3} rounded fill="#fff" />
          <SketchBox x={48} y={y + 8} w={20} h={20} rough={0.2} fill="#f59e0b" />
          <Scribble x={80} y={y + 18} size={13} family="Caveat" weight={700}>{row[0]}</Scribble>
          <Scribble x={80} y={y + 32} size={10} family="JetBrains Mono" color="#888">{row[1]}</Scribble>
          <Scribble x={950} y={y + 24} size={10} family="JetBrains Mono" color="#888">{row[3]}</Scribble>
          {/* relevance dimmed */}
          <Scribble x={1180} y={y + 24} size={10} family="JetBrains Mono" color="#888" opacity={0.4}>0.999</Scribble>
          <Scribble x={1228} y={y + 26} size={14} family="Caveat" anchor="end" color="#888">⋯</Scribble>
        </g>
      );
    })}

    {/* Group: docs */}
    <Scribble x={32} y={500} size={11} family="Kalam" weight={700} color="#888">▾ DOCX · 4</Scribble>
    {[
      ['ООО Спецмаш.docx','Магазин/Карточки/...','142 КБ','создан 03.04 · изм. 07.04'],
      ['ООО Спецмаш (почта).docx','Магазин/Карточки/...','98 КБ','создан 03.04 · изм. 04.04', true],
      ['Карточка предпр. Спецмаш 2026.docx','Магазин/Нужное/...','158 КБ','создан 02.04 · изм. 05.04'],
    ].map((row, ri) => {
      const y = 516 + ri * 44;
      const selected = row[4];
      return (
        <g key={ri}>
          <SketchBox x={32} y={y} w={1216} h={40} rough={0.3} rounded fill={selected ? '#dbeafe' : '#fff'} stroke={selected ? accentInk : inkColor} strokeWidth={selected ? 1.8 : 1} />
          <SketchBox x={48} y={y + 12} w={14} h={14} rough={0.2} fill={selected ? accentInk : '#fff'} />
          {selected && <Scribble x={55} y={y + 23} size={11} family="Caveat" anchor="middle" color="#fff" weight={700}>✓</Scribble>}
          <SketchBox x={72} y={y + 8} w={20} h={20} rough={0.2} fill="#2563eb" />
          <Scribble x={104} y={y + 18} size={13} family="Caveat" weight={600}>{row[0]}</Scribble>
          <Scribble x={104} y={y + 32} size={10} family="JetBrains Mono" color="#888">{row[1]}</Scribble>
          <Scribble x={840} y={y + 24} size={10} family="JetBrains Mono" color="#888">{row[2]}</Scribble>
          <Scribble x={920} y={y + 24} size={10} family="JetBrains Mono" color="#888">{row[3]}</Scribble>
          <Scribble x={1180} y={y + 24} size={10} family="JetBrains Mono" color="#888" opacity={0.4}>0.997</Scribble>
          <Scribble x={1228} y={y + 26} size={14} family="Caveat" anchor="end" color="#444">⋯</Scribble>
        </g>
      );
    })}

    {/* Selection bar */}
    <SketchBox x={32} y={680} w={1216} h={48} rough={0.4} rounded fill="#1d1d1d" />
    <Scribble x={56} y={710} size={13} family="Caveat" weight={700} color="#fff">выбрано: 1</Scribble>
    <Tag x={148} y={696} label="📁 в проводник" color="#fff" w={130} />
    <Tag x={288} y={696} label="⤓ скачать" color="#fff" w={100} />
    <Tag x={398} y={696} label="⭐ избранное" color="#fff" w={110} />
    <Tag x={518} y={696} label="🔗 ссылка" color="#fff" w={90} />
    <Tag x={618} y={696} label="📊 экспорт CSV" color="#fff" w={120} />
    <Scribble x={1228} y={710} size={13} family="Caveat" anchor="end" color="#fff" weight={700}>↑ открыть превью</Scribble>

    {/* Preview drawer hint (collapsed) */}
    <SketchBox x={32} y={744} w={1216} h={36} rough={0.4} rounded fill="#fff" stroke="#ccc" dashed />
    <Scribble x={624} y={764} size={12} family="Caveat" anchor="middle" color="#888" weight={600}>▲ ПРЕВЬЮ — нажмите стрелку или дважды кликните по файлу</Scribble>

    {/* Pagination */}
    <Scribble x={48} y={808} size={11} family="JetBrains Mono" color="#888">показано 1-7 из 50</Scribble>
    <Scribble x={1228} y={808} size={11} family="JetBrains Mono" anchor="end" color="#888">‹ 1 2 3 4 5 ›  ·  на странице ▾ 25</Scribble>

    {/* Three-dot menu callout */}
    <Callout from={[1280, 540]} to={[1232, 540]} label="трёхточечное меню —" side="top" />
    <Scribble x={1280} y={556} size={13} color={accentRed} family="Caveat" weight={600}>все действия с файлом</Scribble>

    <StickyNote x={32} y={832} w={620} h={88} color="#bfdbfe">
      Веса (0.999, 0.997) — полупрозрачные. Главное — даты создания/изменения. Превью скрыто по умолчанию, открывается стрелкой ▲ или двойным кликом. При выборе нескольких — превью показывает агрегированные данные. «Уточнить в найденном» доступно всегда.
    </StickyNote>

    {/* preview drawer expanded sketch */}
    <SketchBox x={680} y={832} w={568} h={88} rough={0.4} rounded fill="#fff" />
    <Scribble x={696} y={854} size={11} family="Kalam" weight={700} color="#888">ТРЁХТОЧЕЧНОЕ МЕНЮ:</Scribble>
    {['открыть','скачать','показать в проводнике','копировать ссылку','добавить в избранное','найти похожие','переиндексировать','свойства','▸ переместить'].map((a, i) => (
      <Scribble key={i} x={696 + (i % 3) * 180} y={874 + Math.floor(i / 3) * 16} size={11} family="Kalam" color="#444">· {a}</Scribble>
    ))}
  </svg>
);

/* === 04 PREVIEW DRAWER OPEN — single file === */
const V2_Preview = () => (
  <svg width="1280" height="780" viewBox="0 0 1280 780">
    <SketchBox x={0} y={0} w={1280} h={780} fill="#f7f4ec" rough={0} />

    {/* compact result row at top */}
    <SketchBox x={32} y={24} w={1216} h={48} rough={0.3} rounded fill="#dbeafe" stroke={accentInk} strokeWidth={1.8} />
    <SketchBox x={56} y={36} w={20} h={24} rough={0.2} fill="#2563eb" />
    <Scribble x={88} y={48} size={14} family="Caveat" weight={700}>ООО Спецмаш (почта, телефон).docx</Scribble>
    <Scribble x={88} y={64} size={10} family="JetBrains Mono" color="#666">Магазин/Карточки/ООО Спецмаш</Scribble>
    <Scribble x={1228} y={52} size={14} family="Caveat" anchor="end" weight={700}>▾ закрыть превью</Scribble>

    {/* drawer */}
    <SketchBox x={32} y={84} w={1216} h={624} rough={0.4} rounded fill="#fff" />

    {/* Tabs in preview */}
    <SketchLine x1={32} y1={120} x2={1248} y2={120} stroke="#eee" />
    {['Превью','Метаданные','Связанные · 8','История','Чанки · 24'].map((t, i) => (
      <g key={i}>
        <Scribble x={56 + i * 130} y={108} size={13} family="Caveat" weight={i === 0 ? 700 : 500} color={i === 0 ? inkColor : '#888'}>{t}</Scribble>
        {i === 0 && <SketchLine x1={40 + i * 130} y1={118} x2={120 + i * 130} y2={118} stroke={inkColor} strokeWidth={2.5} />}
      </g>
    ))}

    {/* Left: doc preview */}
    <SketchBox x={56} y={140} w={680} h={548} rough={0.3} rounded fill="#fafafa" />
    {Array.from({ length: 14 }).map((_, i) => (
      <TextLines key={i} x={88} y={172 + i * 36} count={1} width={616} gap={10} color="#aaa" />
    ))}
    <SketchBox x={88} y={264} w={616} h={4} rough={0.1} fill={accentYellow} opacity={0.7} />
    <Scribble x={88} y={272} size={12} family="Kalam" color="#000" weight={600}>...карточка предприятия Спецмаш...</Scribble>

    {/* Right: meta + actions */}
    <Scribble x={760} y={156} size={13} family="Caveat" weight={700}>Метаданные</Scribble>
    {[
      ['Размер', '98 КБ'],
      ['Тип', '.docx'],
      ['Создан', '03.04.2026'],
      ['Изменён', '04.04.2026 14:23'],
      ['Автор', 'admin'],
      ['Каталог', 'Магазин / Карточки'],
      ['Чанков', '24 (small) · 6 (large)'],
      ['В индексе', '✓ актуален'],
    ].map((m, i) => (
      <g key={i}>
        <Scribble x={760} y={184 + i * 22} size={11} family="Kalam" color="#888">{m[0]}</Scribble>
        <Scribble x={1224} y={184 + i * 22} size={11} family="JetBrains Mono" anchor="end">{m[1]}</Scribble>
        <SketchLine x1={760} y1={188 + i * 22} x2={1224} y2={188 + i * 22} stroke="#f0f0f0" />
      </g>
    ))}

    <SketchLine x1={760} y1={386} x2={1224} y2={386} stroke="#ddd" />
    <Scribble x={760} y={406} size={13} family="Caveat" weight={700}>Действия</Scribble>
    {[
      '↗ открыть в Word',
      '⤓ скачать',
      '📁 показать в проводнике',
      '🔗 копировать ссылку',
      '⭐ добавить в избранное',
      '🔍 найти похожие',
      '🔄 переиндексировать',
    ].map((a, i) => (
      <g key={i}>
        <SketchBox x={760} y={418 + i * 32} w={464} h={26} rough={0.3} rounded fill="#fafafa" />
        <Scribble x={776} y={436 + i * 32} size={12} family="Kalam">{a}</Scribble>
      </g>
    ))}

    <Callout from={[1290, 580]} to={[1226, 580]} label="скрыто за умолчанием —" side="top" />
    <Scribble x={1290} y={596} size={13} color={accentRed} family="Caveat" weight={600}>раскрылось при выборе</Scribble>

    <StickyNote x={32} y={724} w={1216} h={44} color="#bfdbfe">
      МУЛЬТИВЫБОР: при выборе нескольких файлов превью показывает агрегат — общий размер, диапазон дат, пересечение каталогов, общие чанки. Действия применяются ко всем выбранным.
    </StickyNote>
  </svg>
);

/* === 05 EMPTY FOCUS — only dropdown === */
const V2_FocusEmpty = () => (
  <svg width="1100" height="560" viewBox="0 0 1100 560">
    <SketchBox x={0} y={0} w={1100} h={560} fill="#fdfcf7" rough={0} />

    {/* dimmed page background */}
    <SketchBox x={32} y={24} w={1036} h={520} rough={0.2} rounded fill="#fff" opacity={0.3} />
    <Scribble x={48} y={56} size={20} family="Caveat" color="#aaa" opacity={0.4}>фоновая страница затемнена…</Scribble>

    {/* search + autocomplete */}
    <SketchBox x={140} y={120} w={820} h={48} rough={0.5} rounded fill="#fff" stroke={accentInk} strokeWidth={2} />
    <Scribble x={164} y={150} size={15} family="Caveat" color="#aaa">введите запрос…</Scribble>
    <Scribble x={164} y={150} size={15} family="Caveat" weight={600}>|</Scribble>

    {/* Dropdown — ONLY after focus, while empty */}
    <SketchBox x={140} y={176} w={820} h={300} rough={0.4} rounded fill="#fff" stroke="#ddd" />
    <SketchLine x1={550} y1={196} x2={550} y2={460} stroke="#eee" />

    <Scribble x={164} y={208} size={10} family="Kalam" weight={700} color={accentInk}>МОЯ ИСТОРИЯ</Scribble>
    {['карточка предприятия','PC300','паспорт','доверенность Грищенко','vin lovol'].map((q, i) => (
      <g key={i}>
        <Scribble x={180} y={236 + i * 30} size={12} family="Kalam">🕒  {q}</Scribble>
      </g>
    ))}

    <Scribble x={574} y={208} size={10} family="Kalam" weight={700} color={accentInk}>ЧАСТО ИЩУТ</Scribble>
    {['Спецмаш','PC300','Сколько весит PC300','паспорт','vin lovol'].map((q, i) => (
      <g key={i}>
        <Scribble x={590} y={236 + i * 30} size={12} family="Kalam">↗  {q}</Scribble>
      </g>
    ))}

    <Callout from={[1010, 320]} to={[960, 320]} label="ТОЛЬКО при пустом фокусе —" side="top" />
    <Scribble x={1010} y={336} size={13} color={accentRed} family="Caveat" weight={600}>как только начал печатать → автокомплит</Scribble>
  </svg>
);

/* === 06 TYPING AUTOCOMPLETE — predictive === */
const V2_TypingAutocomplete = () => (
  <svg width="1100" height="540" viewBox="0 0 1100 540">
    <SketchBox x={0} y={0} w={1100} h={540} fill="#fdfcf7" rough={0} />

    <SketchBox x={140} y={80} w={820} h={48} rough={0.5} rounded fill="#fff" stroke={accentInk} strokeWidth={2} />
    <Scribble x={164} y={110} size={16} family="Caveat" weight={600}>паспорт ц</Scribble>
    <Scribble x={244} y={110} size={16} family="Caveat" color="#aaa">ыбусов</Scribble>
    <SketchBox x={244} y={92} w={68} h={20} rough={0.2} fill={accentInk} opacity={0.15} />

    <SketchBox x={140} y={136} w={820} h={296} rough={0.4} rounded fill="#fff" stroke="#ddd" />

    {/* предложения */}
    <Scribble x={164} y={168} size={10} family="Kalam" weight={700} color="#888">ПРЕДЛОЖЕНИЯ</Scribble>
    {[
      ['паспорт ц', 'ыбусов', 'имя из истории', 14],
      ['паспорт ц', 'ифрового устройства', 'найдено в 8 файлах', 8],
      ['паспорт ц', 'еха', 'найдено в 3 файлах', 3],
    ].map((s, i) => (
      <g key={i}>
        <SketchBox x={148} y={184 + i * 36} w={804} h={32} rough={0.2} rounded fill={i === 0 ? '#f0f9ff' : '#fff'} stroke={i === 0 ? accentInk : 'none'} />
        <Scribble x={172} y={206 + i * 36} size={14} family="Caveat" weight={600}>🔍  {s[0]}</Scribble>
        <Scribble x={236} y={206 + i * 36} size={14} family="Caveat" weight={700} color={accentInk}>{s[1]}</Scribble>
        <Scribble x={530} y={206 + i * 36} size={11} family="Kalam" color="#888">— {s[2]}</Scribble>
        <Tag x={900} y={194 + i * 36} label={`${s[3]}`} color="#666" w={42} />
      </g>
    ))}

    <SketchLine x1={156} y1={302} x2={944} y2={302} stroke="#eee" />
    <Scribble x={164} y={324} size={10} family="Kalam" weight={700} color="#888">МОЯ ИСТОРИЯ — содержит «ц»</Scribble>
    {['паспорт цыбусов','цех № 4 договор','цементный завод поставка'].map((q, i) => (
      <Scribble key={i} x={180} y={350 + i * 24} size={12} family="Kalam">🕒  {q}</Scribble>
    ))}

    <Callout from={[1080, 200]} to={[948, 200]} label="дополнение фразы —" side="top" />
    <Scribble x={1080} y={216} size={13} color={accentRed} family="Caveat" weight={600}>→ Tab / →</Scribble>
  </svg>
);

/* === 07 EXPLORER — windows-explorer-like with semantic search per folder === */
const V2_Explorer = () => (
  <svg width="1280" height="900" viewBox="0 0 1280 900">
    <SketchBox x={0} y={0} w={1280} h={900} fill="#f7f4ec" rough={0} />

    {/* path bar with breadcrumbs and back/forward/up */}
    <SketchBox x={32} y={24} w={1216} h={36} rough={0.3} rounded fill="#fff" />
    <Scribble x={56} y={48} size={14} family="Caveat" weight={700}>← →   ↑</Scribble>
    <Scribble x={140} y={48} size={13} family="Kalam" weight={500}>📁 Магазин ›  Карточки предпр. ›  ООО Спецмаш</Scribble>
    <Scribble x={1232} y={48} size={13} family="Caveat" anchor="end" color="#666">⟳   ✱ закрепить</Scribble>

    {/* search per-folder + filters bar */}
    <SketchBox x={32} y={72} w={1216} h={48} rough={0.3} rounded fill="#fff" stroke={accentInk} />
    <Scribble x={56} y={102} size={14} family="Kalam" color="#666">🔍 семантический поиск ТОЛЬКО в этой папке…</Scribble>
    <Tag x={760} y={86} label="✓ включая подпапки" color={accentInk} filled />
    <Tag x={904} y={86} label="✓ AI" color={accentInk} filled />
    <Scribble x={1232} y={102} size={11} family="Kalam" anchor="end" color="#888">75 папок · 120 файлов</Scribble>

    {/* === Three columns: tree | files | preview === */}

    {/* LEFT — tree */}
    <SketchBox x={32} y={132} w={260} h={748} rough={0.3} rounded fill="#fff" />
    <SketchBox x={48} y={148} w={228} h={28} rough={0.3} rounded fill="#fafafa" />
    <Scribble x={64} y={166} size={11} family="Kalam" color="#666">🔍 фильтр по дереву…</Scribble>

    <Scribble x={48} y={196} size={10} family="Kalam" weight={700} color="#888">ИЗБРАННОЕ</Scribble>
    {['⭐ Карточки 2026','⭐ Договоры'].map((p, i) => (
      <Scribble key={i} x={56} y={216 + i * 22} size={12} family="Kalam">{p}</Scribble>
    ))}

    <Scribble x={48} y={278} size={10} family="Kalam" weight={700} color="#888">ДЕРЕВО</Scribble>
    {[
      ['▾ 📁 Магазин', 0, true, false],
      ['  ▾ 📁 Карточки предпр.', 1, true, false],
      ['    ▸ 📁 ООО Габдуллин', 2, false, false],
      ['    ▾ 📁 ООО Спецмаш', 2, true, true],
      ['      📁 Доверенности', 3, false, false],
      ['      📁 Реквизиты', 3, false, false],
      ['  ▸ 📁 Раб. стол', 1, false, false],
      ['  ▸ 📁 Нужное', 1, false, false],
      ['  ▸ 📁 Счета', 1, false, false],
      ['▸ 📁 Ритейл', 0, false, false],
      ['▸ 📁 Логистика', 0, false, false],
      ['▸ 📁 Архив 2024', 0, false, false],
      ['▸ 📁 Архив 2025', 0, false, false],
    ].map((n, i) => (
      <g key={i}>
        {n[3] && <SketchBox x={42} y={290 + i * 24} w={244} h={22} rough={0.2} fill={accentInk} opacity={0.12} />}
        <Scribble x={56} y={306 + i * 24} size={11} family="JetBrains Mono" weight={n[3] ? 700 : 500} color={n[3] ? accentInk : '#444'}>
          {n[0]}
        </Scribble>
      </g>
    ))}

    {/* === MIDDLE — file list with windows-style columns === */}
    <SketchBox x={304} y={132} w={672} h={748} rough={0.3} rounded fill="#fff" />

    {/* Toolbar (windows explorer-like) */}
    <SketchLine x1={304} y1={172} x2={976} y2={172} stroke="#eee" />
    {['☰ список','▦ значки','⊞ плитка','☷ детали','⊟ компакт'].map((v, i) => (
      <Scribble key={i} x={320 + i * 72} y={160} size={11} family="Kalam" weight={i === 3 ? 700 : 500} color={i === 3 ? accentInk : '#666'}>{v}</Scribble>
    ))}
    <Scribble x={696} y={160} size={11} family="Kalam" color="#888">сорт:</Scribble>
    <Tag x={736} y={148} label="имя ↑" color={inkColor} />
    <Scribble x={812} y={160} size={11} family="Kalam" color="#888">группа:</Scribble>
    <Tag x={868} y={148} label="нет" color="#666" />
    <Scribble x={960} y={160} size={11} family="Kalam" anchor="end" color={accentInk} weight={700}>⊞ настроить вид</Scribble>

    {/* Column headers (windows-style sortable) */}
    <SketchLine x1={304} y1={206} x2={976} y2={206} stroke="#1d1d1d" strokeWidth={1.5} />
    {[
      ['Имя', 320, 200],
      ['Тип', 540, 80],
      ['Изменён', 624, 110],
      ['Размер', 740, 80],
      ['Автор', 824, 90],
      ['Индекс', 920, 56],
    ].map((c, i) => (
      <g key={i}>
        <Scribble x={c[1]} y={196} size={11} family="Kalam" weight={700} color="#444">{c[0]} {i === 0 ? '↑' : ''}</Scribble>
        {i > 0 && <SketchLine x1={c[1] - 4} y1={184} x2={c[1] - 4} y2={204} stroke="#eee" />}
      </g>
    ))}

    {/* Rows */}
    {Array.from({ length: 18 }).map((_, ri) => {
      const types = ['folder','folder','pdf','doc','xls','folder','pdf','doc','folder','img','pdf','doc','folder','folder','xls','doc','pdf','folder'];
      const names = [
        '📁 1Фактуры ТСК','📁 Анастасия диспетчер','📕 Бланки путевок.pdf','📄 ВТН 30.06.2025.docx',
        '📊 Выписка из банка.pdf','📁 Голгофа Татьяна','📕 Грузовые автомобили.pdf','📄 ГТСК.docx',
        '📁 Для Дмитрия','🖼 Для Евгении.jpg','📕 ДЛЯ ЗАПИСИ В.pdf','📄 для регистрации.docx',
        '📁 ДЛЯ СЕРГЕЙ','📁 Договор для магазина','📊 Договора инвент.xlsx','📄 Документы.docx',
        '📕 Документы водителей.pdf','📁 Документы ИП Демидов'
      ];
      const sizes = ['—','—','142 КБ','98 КБ','220 КБ','—','1.2 МБ','64 КБ','—','340 КБ','340 КБ','158 КБ','—','—','85 КБ','108 КБ','420 КБ','—'];
      const authors = ['admin','test_search','admin','admin','alex','admin','admin','test_search','admin','admin','admin','admin','admin','admin','admin','admin','admin','admin'];
      const y = 222 + ri * 32;
      const sel = ri === 7;
      return (
        <g key={ri}>
          {sel && <SketchBox x={304} y={y - 14} w={672} h={28} rough={0.2} fill={accentInk} opacity={0.15} />}
          <SketchLine x1={304} y1={y + 10} x2={976} y2={y + 10} stroke="#f5f5f5" />
          <Scribble x={320} y={y} size={11} family="Caveat" weight={sel ? 700 : 500}>{names[ri]}</Scribble>
          <Scribble x={540} y={y} size={10} family="JetBrains Mono" color="#888">{types[ri]}</Scribble>
          <Scribble x={624} y={y} size={10} family="JetBrains Mono" color="#888">{((ri % 28) + 1).toString().padStart(2, '0')}.04 14:{(15 + ri).toString().padStart(2, '0')}</Scribble>
          <Scribble x={740} y={y} size={10} family="JetBrains Mono" color="#888">{sizes[ri]}</Scribble>
          <Scribble x={824} y={y} size={10} family="JetBrains Mono" color="#888">{authors[ri]}</Scribble>
          <Tag x={920} y={y - 10} label={ri % 5 === 0 ? '○' : '✓'} color={ri % 5 === 0 ? '#888' : '#16a34a'} w={28} />
        </g>
      );
    })}

    {/* Status bar */}
    <SketchLine x1={304} y1={808} x2={976} y2={808} stroke="#ddd" />
    <Scribble x={320} y={830} size={10} family="JetBrains Mono" color="#888">75 папок, 120 файлов · 4.2 ГБ</Scribble>
    <Scribble x={960} y={830} size={10} family="JetBrains Mono" anchor="end" color="#888">выбрано: 1 · 64 КБ</Scribble>

    {/* === RIGHT — filters + properties === */}
    <SketchBox x={988} y={132} w={260} h={748} rough={0.3} rounded fill="#fff" />

    <Spoiler x={988} y={132} w={260} h={224} title="Фильтры" count="3 активных" h={224}>
      <Scribble x={1004} y={188} size={10} family="Kalam" color="#888" weight={700}>ТИП</Scribble>
      {['📁 папки · 12','📄 docx · 18','📕 pdf · 8'].map((t, i) => (
        <g key={i}>
          <SketchBox x={1004} y={200 + i * 22} w={12} h={12} rough={0.2} fill={i < 2 ? inkColor : '#fff'} />
          <Scribble x={1022} y={210 + i * 22} size={11} family="Kalam">{t}</Scribble>
        </g>
      ))}
      <Scribble x={1004} y={282} size={10} family="Kalam" color="#888" weight={700}>ИЗМЕНЁН</Scribble>
      {['неделя','месяц','год','любой'].map((t, i) => (
        <g key={i}>
          <Circle cx={1012} cy={298 + i * 18} r={4} fill={i === 0 ? inkColor : 'none'} stroke={inkColor} />
          <Scribble x={1022} y={302 + i * 18} size={11} family="Kalam">{t}</Scribble>
        </g>
      ))}
    </Spoiler>

    <Spoiler x={988} y={368} w={260} h={284} title="Свойства файла" count="ГТСК.docx" h={284}>
      {[
        ['Тип', '.docx'],
        ['Размер', '64 КБ'],
        ['Создан', '03.04.26'],
        ['Изменён', '04.04.26 14:22'],
        ['Автор', 'admin'],
        ['В индексе', '✓'],
        ['Чанков', '12 / 4'],
      ].map((m, i) => (
        <g key={i}>
          <Scribble x={1004} y={416 + i * 24} size={11} family="Kalam" color="#888">{m[0]}</Scribble>
          <Scribble x={1232} y={416 + i * 24} size={11} family="JetBrains Mono" anchor="end">{m[1]}</Scribble>
        </g>
      ))}
    </Spoiler>

    <Spoiler x={988} y={664} w={260} h={216} title="Группировка" count="кастомизация" h={216}>
      <Scribble x={1004} y={714} size={10} family="Kalam" color="#888" weight={700}>ГРУППИРОВКА</Scribble>
      {['нет','по типу','по дате','по автору','по индексу'].map((g, i) => (
        <g key={i}>
          <Circle cx={1012} cy={730 + i * 22} r={4} fill={i === 0 ? inkColor : 'none'} stroke={inkColor} />
          <Scribble x={1022} y={734 + i * 22} size={11} family="Kalam">{g}</Scribble>
        </g>
      ))}
      <Scribble x={1004} y={862} size={11} family="Caveat" color={accentInk} weight={700}>+ настроить колонки…</Scribble>
    </Spoiler>

    <Callout from={[10, 76]} to={[34, 96]} label="семантический —" side="top" />
    <Scribble x={10} y={92} size={13} color={accentRed} family="Caveat" weight={600}>в папке</Scribble>
  </svg>
);

/* === 08 INDEX — phases with timing, OCR as phase, pause/cancel === */
const V2_Index = () => (
  <svg width="1280" height="900" viewBox="0 0 1280 900">
    <SketchBox x={0} y={0} w={1280} h={900} fill="#f7f4ec" rough={0} />
    <Scribble x={32} y={48} size={26} family="Caveat" weight={700}>Индексация</Scribble>
    <Tag x={184} y={36} label="● running" color={accentInk} filled />

    <Tag x={1080} y={36} label="⏸ пауза" color={inkColor} w={70} />
    <Tag x={1158} y={36} label="✕ отмена" color={accentRed} w={88} />

    {/* === PHASES with start time + duration === */}
    <Scribble x={32} y={92} size={16} family="Caveat" weight={700}>Этапы индексации</Scribble>
    <Scribble x={1248} y={92} size={11} family="Kalam" anchor="end" color="#888">→ время старта · длительность</Scribble>

    {[
      { n: 'metadata', s: 'ok', ts: '02:00:00', dur: '12 мин', age: 'свежий', recent: 'green', files: '85 193', units: 'файлов' },
      { n: 'small', s: 'failed', ts: '02:12:00', dur: '49 мин', age: 'свежий', recent: 'green', files: '7 848', units: 'файлов', err: '1408' },
      { n: 'large', s: 'running', ts: '03:01:00', dur: '13 мин / ~30', age: 'идёт', recent: 'blue', files: '328 / 11 800', units: 'файлов' },
      { n: 'content', s: 'pending', ts: '—', dur: '—', age: '—', recent: 'empty', files: '—', units: 'чанки' },
      { n: 'OCR', s: 'pending', ts: 'было: 22.04 04:00', dur: '6ч 12м', age: 'давно', recent: 'red', files: '849 PDF', units: 'документов · 4280 страниц' },
    ].map((p, i) => {
      const y = 124 + i * 88;
      const colors = { ok: '#16a34a', failed: accentRed, running: accentInk, pending: '#888' };
      const recentColors = { green: '#16a34a', blue: accentInk, red: accentRed, empty: '#ddd' };
      return (
        <g key={i}>
          <SketchBox x={32} y={y} w={1216} h={76} rough={0.3} rounded fill="#fff" stroke={p.s === 'running' ? accentInk : '#eee'} strokeWidth={p.s === 'running' ? 1.8 : 1} />

          {/* Status circle */}
          <Circle cx={64} cy={y + 38} r={14} fill={colors[p.s]} />
          <Scribble x={64} y={y + 44} size={14} family="Caveat" anchor="middle" weight={700} color="#fff">
            {p.s === 'ok' ? '✓' : p.s === 'failed' ? '✗' : p.s === 'running' ? '◷' : i + 1}
          </Scribble>

          <Scribble x={92} y={y + 28} size={18} family="Caveat" weight={700}>{p.n}</Scribble>
          <Tag x={92 + p.n.length * 12} y={y + 14} label={p.s} color={colors[p.s]} filled />
          {p.err && <Scribble x={92 + p.n.length * 12 + 80} y={y + 24} size={11} family="JetBrains Mono" color={accentRed}>· {p.err} ошибок</Scribble>}

          <Scribble x={92} y={y + 50} size={11} family="JetBrains Mono" color="#888">старт: {p.ts}  ·  длительность: {p.dur}  ·  {p.files} {p.units}</Scribble>

          {/* Recency bar — RED long-ago, GREEN recent, BLUE running, EMPTY never */}
          <Scribble x={92} y={y + 66} size={10} family="Kalam" color="#666" weight={700}>СВЕЖЕСТЬ:</Scribble>
          <SketchBox x={156} y={y + 58} w={400} h={10} rough={0.2} rounded
            fill={recentColors[p.recent]} opacity={p.recent === 'empty' ? 0.4 : 0.9} />
          <Scribble x={566} y={y + 66} size={10} family="Kalam" color="#666">{p.age}</Scribble>

          {/* Per-phase progress (only for running) */}
          {p.s === 'running' && (
            <g>
              <SketchBox x={620} y={y + 32} w={400} h={12} rough={0.2} rounded fill="#eee" />
              <SketchBox x={620} y={y + 32} w={11} h={12} rough={0.2} rounded fill={accentInk} />
              <Scribble x={1030} y={y + 42} size={10} family="JetBrains Mono" color="#888">2.8%</Scribble>
            </g>
          )}

          {/* per-phase actions */}
          <Scribble x={1228} y={y + 28} size={11} family="Kalam" anchor="end" color={accentInk} weight={700}>
            {p.s === 'running' ? '⏸ пауза' : p.s === 'pending' ? '▶ запустить' : '↻ перезапустить'}
          </Scribble>
          <Scribble x={1228} y={y + 48} size={11} family="Kalam" anchor="end" color="#666">детали · логи</Scribble>
        </g>
      );
    })}

    {/* OCR sub-stats */}
    <Spoiler x={32} y={580} w={1216} h={140} title="OCR — отдельный счёт по документам и страницам" count="особый этап" h={140}>
      <Scribble x={48} y={628} size={11} family="Kalam" color="#888" weight={700}>В отличие от других этапов считается:</Scribble>
      {[
        ['Документов', '849', 'PDF / TIFF / JPG'],
        ['Страниц', '4 280', '~5 страниц / документ'],
        ['Распознано', '4 152', '97% успешно'],
        ['Ошибок OCR', '128', 'низкое качество скана'],
      ].map((m, i) => (
        <g key={i}>
          <Scribble x={48 + i * 304} y={668} size={10} family="Kalam" color="#888" weight={700}>{m[0].toUpperCase()}</Scribble>
          <Scribble x={48 + i * 304} y={696} size={22} family="Caveat" weight={700}>{m[1]}</Scribble>
          <Scribble x={140 + i * 304} y={696} size={11} family="Kalam" color="#666">{m[2]}</Scribble>
        </g>
      ))}
    </Spoiler>

    {/* Schedule + params */}
    <Spoiler x={32} y={736} w={600} h={148} title="📅 Расписание" count="3 регламента" h={148}>
      {[
        ['Утренний инкремент', '⏱ 03:00 пн-пт', 'metadata · small'],
        ['Полный пересчёт', '⏱ 02:00 ежедневно', 'все этапы'],
        ['OCR ночь', '⏱ 04:00 сб', 'только OCR'],
      ].map((s, i) => (
        <g key={i}>
          <SketchLine x1={48} y1={802 + i * 28} x2={616} y2={802 + i * 28} stroke="#eee" />
          <Circle cx={56} cy={794 + i * 28} r={5} fill="#16a34a" />
          <Scribble x={70} y={798 + i * 28} size={12} family="Kalam" weight={600}>{s[0]}</Scribble>
          <Scribble x={250} y={798 + i * 28} size={11} family="JetBrains Mono" color="#666">{s[1]}</Scribble>
          <Tag x={420} y={788 + i * 28} label={s[2]} color="#666" w={170} />
        </g>
      ))}
    </Spoiler>

    <Spoiler x={648} y={736} w={600} h={148} title="⚙ Параметры" count="6 настроек" h={148}>
      <Scribble x={664} y={788} size={11} family="Kalam" color="#888">Размер чанков (small / large)</Scribble>
      <Scribble x={1232} y={788} size={11} family="JetBrains Mono" anchor="end">512  /  2 000</Scribble>
      <Scribble x={664} y={812} size={11} family="Kalam" color="#888">Макс. размер файла</Scribble>
      <Scribble x={1232} y={812} size={11} family="JetBrains Mono" anchor="end">4 МБ</Scribble>
      <Scribble x={664} y={836} size={11} family="Kalam" color="#888">DPI для OCR</Scribble>
      <Scribble x={1232} y={836} size={11} family="JetBrains Mono" anchor="end">300</Scribble>
      <Scribble x={664} y={860} size={11} family="Kalam" color="#888">Параллельных воркеров</Scribble>
      <Scribble x={1232} y={860} size={11} family="JetBrains Mono" anchor="end">8</Scribble>
    </Spoiler>

    <Callout from={[600, 168]} to={[556, 184]} label="свежесть —" side="top" />
    <Scribble x={600} y={184} size={13} color={accentRed} family="Caveat" weight={600}>зелёный=свежий, красный=давно, синий=идёт</Scribble>
  </svg>
);

/* === 09 ANALYTICS — D's table + B's mini-bars per row === */
const V2_Analytics = () => (
  <svg width="1280" height="940" viewBox="0 0 1280 940">
    <SketchBox x={0} y={0} w={1280} h={940} fill="#f7f4ec" rough={0} />

    <Scribble x={32} y={48} size={26} family="Caveat" weight={700}>Аналитика</Scribble>
    {/* sub-tabs */}
    <Tag x={32} y={66} label="Обзор" color={inkColor} filled />
    <Tag x={104} y={66} label="Запросы" color="#666" />
    <Tag x={188} y={66} label="Пользователи" color="#666" />
    <Tag x={296} y={66} label="Производительность" color="#666" />
    <Tag x={444} y={66} label="Аудит" color="#666" />
    <Tag x={508} y={66} label="Ошибки" color="#666" />
    <Tag x={572} y={66} label="+ свой раздел" color={accentInk} filled />

    <Tag x={1080} y={36} label="неделя ▾" color={inkColor} />
    <Tag x={1158} y={36} label="⤓ CSV / PDF" color="#666" />

    {/* === KPI tiles WITH mini-bars (B's idea) === */}
    {[
      { l: 'Запросов', v: '187', d: '+23%', bars: [12,35,70,92,28,18,48] },
      { l: 'Уникальных', v: '12', d: '+1', bars: [3,5,7,8,4,2,6] },
      { l: 'Время отклика', v: '0.42с', d: '-12%', bars: [50,48,46,42,40,42,42] },
      { l: 'AI вызовов', v: '94', d: '+45%', bars: [6,18,40,50,14,9,24] },
      { l: 'Успешных', v: '94%', d: '+2%', bars: [88,90,91,92,93,94,94] },
    ].map((m, i) => (
      <g key={i}>
        <SketchBox x={32 + i * 244} y={106} w={224} h={120} rough={0.3} rounded fill="#fff" />
        <Scribble x={48 + i * 244} y={128} size={10} family="Kalam" color="#888" weight={700}>{m.l.toUpperCase()}</Scribble>
        <Scribble x={48 + i * 244} y={160} size={26} family="Caveat" weight={700}>{m.v}</Scribble>
        <Tag x={48 + i * 244} y={170} label={m.d} color={m.d.startsWith('-') && i < 2 ? accentRed : '#16a34a'} filled w={50} />
        {/* mini bars */}
        {m.bars.map((b, j) => (
          <SketchBox key={j} x={148 + i * 244 + j * 12} y={216 - b * 0.6} w={8} h={Math.max(b * 0.6, 2)} rough={0.1} fill={accentInk} opacity={0.6} />
        ))}
      </g>
    ))}

    {/* AI insight panel */}
    <SketchBox x={32} y={246} w={1216} h={48} rough={0.3} rounded fill={accentYellow} opacity={0.4} stroke="#a89020" />
    <Scribble x={48} y={278} size={11} family="Kalam" color="#a89020" weight={700}>🤖 AI:</Scribble>
    <Scribble x={88} y={278} size={12} family="Kalam">Запросы выросли в среду на 92 — после совещания у admin. Замечен пик AI-запросов: «PC300» (16 раз). Команда test_search неактивна 4 дня.</Scribble>
    <Scribble x={1232} y={278} size={11} family="Kalam" anchor="end" color="#888">сводка автоматическая · можно отключить</Scribble>

    {/* === Configurable table (D-style) — pro tool === */}
    <Scribble x={32} y={324} size={16} family="Caveat" weight={700}>История запросов</Scribble>
    <Scribble x={1232} y={324} size={11} family="Kalam" anchor="end" color={accentInk} weight={700}>⊞ настроить колонки  ·  💾 сохранить пресет  ·  ⤓ CSV</Scribble>

    {/* filters row */}
    <SketchBox x={32} y={336} w={1216} h={36} rough={0.3} rounded fill="#fff" />
    <SketchInput x={48} y={344} w={140} h={20} label="источник ▾" />
    <SketchInput x={196} y={344} w={140} h={20} label="пользователь ▾" />
    <SketchInput x={344} y={344} w={140} h={20} label="статус ▾" />
    <SketchInput x={492} y={344} w={140} h={20} label="есть AI ▾" />
    <SketchInput x={640} y={344} w={300} h={20} label="🔍 запрос содержит…" />
    <Scribble x={1232} y={358} size={11} family="Kalam" anchor="end" color="#888">фильтр работает над всеми 7 пресетами</Scribble>

    {/* table */}
    <SketchBox x={32} y={384} w={1216} h={508} rough={0.3} rounded fill="#fff" />
    <SketchLine x1={32} y1={416} x2={1248} y2={416} stroke="#1d1d1d" strokeWidth={1.5} />
    {['Время','Источник','Пользователь','Запрос','Результаты','Время','AI','Статус'].map((h, i) => {
      const xs = [48, 160, 264, 408, 868, 952, 1024, 1100];
      return (
        <g key={i}>
          <Scribble x={xs[i]} y={406} size={11} family="Kalam" weight={700} color="#444">{h} ↕</Scribble>
          {i > 0 && <SketchLine x1={xs[i] - 6} y1={392} x2={xs[i] - 6} y2={414} stroke="#eee" />}
        </g>
      );
    })}

    {/* Rows with mini-bars on duration column */}
    {[
      ['28.04 16:25','nicegui','admin','карточка предприятия','50','420мс','✓','ok'],
      ['28.04 16:08','nicegui','admin','PC300','16','280мс','✓','ok'],
      ['28.04 15:14','nicegui','admin','Спецмаш PC300','14','340мс','✓','ok'],
      ['28.04 14:48','nicegui','admin','паспорт','11','220мс','—','ok'],
      ['28.04 14:23','telegram','test_search','vin lovol','8','510мс','✓','ok'],
      ['27.04 14:55','nicegui','admin','карточка предприятия','50','390мс','✓','ok'],
      ['27.04 12:08','nicegui','alex_redux','доверенность','3','620мс','—','ok'],
      ['22.04 12:54','nicegui','admin','Список документов...','—','120мс','—','warn'],
      ['22.04 12:45','telegram','check','PC300','16','290мс','✓','ok'],
      ['21.04 18:12','api','test_search','паспорта','22','180мс','—','ok'],
      ['21.04 17:30','nicegui','admin','цех 4','5','410мс','—','ok'],
    ].map((row, ri) => {
      const xs = [48, 160, 264, 408, 868, 952, 1024, 1100];
      const y = 442 + ri * 38;
      return (
        <g key={ri}>
          <SketchLine x1={32} y1={y + 18} x2={1248} y2={y + 18} stroke="#f5f5f5" />
          {row.map((v, i) => {
            if (i === 5) {
              const ms = parseInt(v);
              return (
                <g key={i}>
                  <Scribble x={xs[i]} y={y + 8} size={10} family="JetBrains Mono" color="#444">{v}</Scribble>
                  <SketchBox x={xs[i]} y={y + 12} w={Math.min(ms / 8, 60)} h={4} rough={0.1} fill={ms > 400 ? '#f59e0b' : '#16a34a'} />
                </g>
              );
            }
            return (
              <Scribble key={i} x={xs[i]} y={y + 8} size={10} family="JetBrains Mono"
                color={i === 7 && v === 'warn' ? accentRed : i === 6 && v === '✓' ? '#16a34a' : '#444'}>{v}</Scribble>
            );
          })}
          <Scribble x={1228} y={y + 10} size={12} family="Caveat" anchor="end" color="#666">⋯</Scribble>
        </g>
      );
    })}

    <Scribble x={48} y={912} size={11} family="JetBrains Mono" color="#888">показано 1-11 из 187</Scribble>
    <Scribble x={1232} y={912} size={11} family="JetBrains Mono" anchor="end" color="#888">‹ 1 2 3 … 17 ›  ·  на странице ▾ 25</Scribble>

    <Callout from={[40, 226]} to={[80, 222]} label="мини-бары в KPI —" side="top" />
    <Scribble x={40} y={242} size={13} color={accentRed} family="Caveat" weight={600}>динамика наглядна</Scribble>
  </svg>
);

const V2 = () => (
  <DCSection id="v2" title="v2 — учтены комментарии Dmitry"
             subtitle="Все блоки сворачиваемые · windows-explorer + семантика · превью скрыто · OCR-этап · автокомплит · «запросить доступ» · мини-бары в аналитике">
    <DCArtboard id="v2-login" label="01 — Вход (B + A форма)" width={1100} height={640}><V2_Login /></DCArtboard>
    <DCArtboard id="v2-home" label="02 — Главная (статистика + история, виджеты)" width={1280} height={900}><V2_Home /></DCArtboard>
    <DCArtboard id="v2-focus-empty" label="03 — Пустой фокус (только дропдаун)" width={1100} height={560}><V2_FocusEmpty /></DCArtboard>
    <DCArtboard id="v2-typing" label="04 — Автодополнение фраз при печати" width={1100} height={540}><V2_TypingAutocomplete /></DCArtboard>
    <DCArtboard id="v2-search" label="05 — Поиск (статистика, группы, уточнить, сорт)" width={1280} height={940}><V2_Search /></DCArtboard>
    <DCArtboard id="v2-preview" label="06 — Превью раскрыто (drawer)" width={1280} height={780}><V2_Preview /></DCArtboard>
    <DCArtboard id="v2-explorer" label="07 — Файлы (как Windows Explorer + семантика в папке)" width={1280} height={900}><V2_Explorer /></DCArtboard>
    <DCArtboard id="v2-index" label="08 — Индекс (этапы, OCR, время, пауза/отмена)" width={1280} height={900}><V2_Index /></DCArtboard>
    <DCArtboard id="v2-analytics" label="09 — Аналитика (D + B мини-бары + AI сводка)" width={1280} height={940}><V2_Analytics /></DCArtboard>
  </DCSection>
);

window.V2 = V2;
