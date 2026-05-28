"""
css.py — Global CSS and JS injection for the NiceGUI app.

Call _install_css() once inside a @ui.page handler before rendering content.
"""

from __future__ import annotations

from pathlib import Path

from nicegui import ui


_INTERACTION_SCRIPT_CACHE = ""
INTERACTION_JS_PATH = Path(__file__).with_name("static") / "rag_interactions.js"


def _install_interaction_javascript() -> None:
    """Install client-side interaction helpers for the current NiceGUI client."""
    if _INTERACTION_SCRIPT_CACHE:
        ui.run_javascript(_INTERACTION_SCRIPT_CACHE)


def _install_css() -> None:
    global _INTERACTION_SCRIPT_CACHE

    ui.add_head_html('<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">')
    ui.add_head_html('<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">')
    ui.add_head_html('<script defer src="/rag-interactions.js"></script>')
    ui.add_head_html("""<style>
        :root {
          --rag-font-display: 'Manrope', system-ui, -apple-system, sans-serif;
          --rag-font-text: 'Inter', system-ui, sans-serif;
          --rag-font-mono: 'JetBrains Mono', ui-monospace, monospace;
          --rag-bg: #fafaf7;
          --rag-surface: #ffffff;
          --rag-surface-strong: #ffffff;
          --rag-sunken: #f4f3ee;
          --rag-border: #e6e3da;
          --rag-border-strong: #d8d3c4;
          --rag-text: #14141a;
          --rag-muted: #6c6c78;
          --rag-muted-2: #9a9aa2;
          --rag-hover: rgba(20, 20, 26, 0.06);
          --rag-accent: #3d63ff;
          --rag-accent-hover: #2949e6;
          --rag-accent-2: #10b981;
          --rag-danger: #dc2626;
          --rag-warn: #f59e0b;
          --rag-header-bg: rgba(250, 250, 247, 0.88);
          --rag-drawer-bg: #ffffff;
          --rag-search-bg: #ffffff;
          --rag-suggest-bg: #ffffff;
          --rag-chip-bg: #ffffff;
          --rag-group-bg: #ffffff;
          --rag-bookmark-bg: #ffffff;
          --rag-bookmark-hover-bg: #f4f3ee;
          --rag-bookmark-remove-bg: #ffffff;
          --rag-bookmark-remove-hover-bg: #fff1f1;
          --rag-context-bg: #ffffff;
          --rag-code-bg: #f4f3ee;
          --rag-shadow: 0 8px 24px -18px rgba(20, 20, 26, 0.34);
        }
        body.body--dark {
          --rag-bg: #0c0c0f;
          --rag-surface: #15151a;
          --rag-surface-strong: #1b1b22;
          --rag-sunken: #08080a;
          --rag-border: #23232b;
          --rag-border-strong: #2e2e38;
          --rag-text: #f4f4f7;
          --rag-muted: #8a8a96;
          --rag-muted-2: #5a5a64;
          --rag-hover: rgba(255, 255, 255, 0.06);
          --rag-accent: #6385ff;
          --rag-accent-hover: #4f6dff;
          --rag-header-bg: rgba(12, 12, 15, 0.9);
          --rag-drawer-bg: #15151a;
          --rag-search-bg: #15151a;
          --rag-suggest-bg: #15151a;
          --rag-chip-bg: #1b1b22;
          --rag-group-bg: #15151a;
          --rag-bookmark-bg: #15151a;
          --rag-bookmark-hover-bg: #23232b;
          --rag-bookmark-remove-bg: #15151a;
          --rag-bookmark-remove-hover-bg: rgba(127, 29, 29, 0.42);
          --rag-context-bg: #15151a;
          --rag-code-bg: #08080a;
          --rag-shadow: 0 12px 32px -20px rgba(0, 0, 0, 0.8);
          background-image: none;
        }
        body {
          background: var(--rag-bg);
          color: var(--rag-text);
          font-family: var(--rag-font-text);
          font-size: 87.5%;
          letter-spacing: 0;
          background-image: none;
          overflow-x: hidden;
        }
        .material-icons,
        .q-icon.material-icons,
        i.q-icon.notranslate {
          font-family: 'Material Icons' !important;
          font-weight: normal;
          font-style: normal;
          font-size: 24px;
          line-height: 1;
          letter-spacing: normal;
          text-transform: none;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          white-space: nowrap;
          word-wrap: normal;
          direction: ltr;
          -webkit-font-feature-settings: 'liga';
          -webkit-font-smoothing: antialiased;
          font-feature-settings: 'liga';
        }
        .q-layout,
        .q-page-container,
        .q-page {
          overflow-x: hidden;
        }
        .q-page,
        .q-drawer,
        .q-drawer__content {
          background: var(--rag-bg);
          color: var(--rag-text);
        }
        .q-field__control,
        .q-menu,
        .q-list,
        .q-table__container,
        .q-table__top,
        .q-table__bottom,
        .q-card {
          background: var(--rag-surface);
          color: var(--rag-text);
        }
        .q-field__native,
        .q-field__input,
        .q-field__label,
        .q-item,
        .q-table,
        .q-table th,
        .q-table td {
          color: var(--rag-text);
        }
        .q-field--outlined .q-field__control:before {
          border-color: var(--rag-border);
        }
        .q-separator {
          background: var(--rag-border);
        }
        .rag-header {
          height: 48px !important;
          min-height: 48px !important;
          max-height: 48px !important;
          background: var(--rag-header-bg);
          color: var(--rag-text);
          border-bottom: 1px solid var(--rag-border);
          backdrop-filter: blur(16px) saturate(140%);
          -webkit-backdrop-filter: blur(16px) saturate(140%);
          display: flex;
          align-items: center;
          overflow: hidden;
        }
        .rag-header > .q-toolbar,
        .rag-header .q-toolbar,
        .rag-header .nicegui-content {
          height: 48px !important;
          min-height: 48px !important;
          align-items: center;
        }
        .rag-header .q-btn,
        .rag-header-button,
        .rag-drawer .q-btn {
          color: var(--rag-text) !important;
        }
        .rag-header .q-btn {
          height: 32px !important;
          min-height: 32px !important;
          max-height: 32px !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
          align-self: center;
        }
        .rag-header .q-btn--round {
          width: 32px !important;
          min-width: 32px !important;
        }
        .rag-header .q-btn__content {
          min-height: 0 !important;
          height: 32px;
          line-height: 1;
          align-items: center;
          justify-content: center;
          flex-wrap: nowrap;
        }
        .rag-header .q-icon {
          line-height: 1;
          font-size: 20px;
        }
        .rag-header .q-img,
        .rag-header img {
          display: block;
          flex: 0 0 auto;
        }
        .rag-header-breadcrumbs,
        .rag-header-actions {
          height: 32px;
          align-items: center;
        }
        .rag-header-breadcrumbs:empty,
        .rag-header-actions:empty {
          display: none;
        }
        .rag-header-breadcrumbs .q-btn { min-height: 32px; padding: 0 6px; }
        .rag-header-actions .q-btn { min-width: 32px; min-height: 32px; }
        .rag-drawer {
          background: var(--rag-drawer-bg);
          border-right: 1px solid var(--rag-border);
        }
        .rag-drawer-body {
          min-height: calc(100vh - 92px);
          display: flex;
          flex-direction: column;
        }
        .rag-drawer-bottom {
          margin-top: auto;
          padding-top: 12px;
          border-top: 1px solid var(--rag-border);
        }
        .rag-page {
          width: min(1440px, calc(100vw - 24px));
          margin: 0 auto;
          padding: 10px 0 32px;
        }
        .rag-page-content {
          transition: min-height .34s ease, padding .34s ease, justify-content .34s ease;
        }
        .rag-page.search {
          padding-top: 4px;
          transition: min-height .34s ease, padding .34s ease;
        }
        .rag-page.search .rag-page-content {
          align-items: center;
        }
        .rag-page.search-empty {
          min-height: calc(100dvh - 58px);
        }
        .rag-page.search-empty .rag-page-content {
          min-height: calc(100dvh - 96px);
          justify-content: center;
          padding-bottom: 0;
        }
        .rag-page.search-active .rag-page-content {
          min-height: 0;
          justify-content: flex-start;
          padding-bottom: 0;
        }
        .rag-search-header {
          width: 100%;
          max-width: 1024px;
          margin-inline: auto;
          transform: translateY(0);
          transition: transform .34s cubic-bezier(.2, .8, .2, 1), max-width .2s ease;
        }
        .rag-page.search-active .rag-search-header {
          animation: rag-search-rise .26s cubic-bezier(.2, .8, .2, 1);
        }
        .rag-search-presets {
          width: min(682px, 66.666%) !important;
          max-width: 682px;
          margin-inline: auto;
          justify-content: center;
          align-items: stretch;
          flex-wrap: wrap;
        }
        .rag-search-presets .q-btn {
          flex: 1 1 104px;
          min-width: 92px;
          max-width: 142px;
        }
        @keyframes rag-search-rise {
          from { transform: translateY(18px); opacity: .96; }
          to { transform: translateY(0); opacity: 1; }
        }
        .rag-global-busy {
          position: fixed;
          left: 50%;
          bottom: 18px;
          z-index: 2300;
          display: none;
          align-items: center;
          gap: 10px;
          width: min(420px, calc(100vw - 28px));
          min-height: 44px;
          padding: 8px 12px;
          border: 1px solid var(--rag-border-strong);
          border-radius: 8px;
          color: var(--rag-text);
          background: color-mix(in srgb, var(--rag-surface) 94%, transparent);
          box-shadow: 0 18px 42px -24px rgba(0,0,0,.62);
          transform: translate(-50%, 12px);
          opacity: 0;
          pointer-events: none;
          backdrop-filter: blur(14px);
          transition: opacity .14s ease, transform .14s ease;
        }
        .rag-global-busy.show {
          display: flex;
          opacity: 1;
          transform: translate(-50%, 0);
        }
        .rag-busy-spinner {
          width: 18px;
          height: 18px;
          border: 2px solid color-mix(in srgb, var(--rag-accent) 22%, transparent);
          border-top-color: var(--rag-accent);
          border-radius: 999px;
          animation: rag-spin .75s linear infinite;
          flex: 0 0 auto;
        }
        .rag-busy-content {
          display: grid;
          gap: 5px;
          min-width: 0;
          flex: 1;
        }
        .rag-busy-label {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          font-size: 13px;
          font-weight: 500;
        }
        .rag-busy-skeleton {
          height: 5px;
          border-radius: 999px;
          overflow: hidden;
          background: color-mix(in srgb, var(--rag-border) 56%, transparent);
        }
        .rag-busy-skeleton::before {
          content: "";
          display: block;
          width: 42%;
          height: 100%;
          border-radius: inherit;
          background: linear-gradient(90deg, transparent, var(--rag-accent), transparent);
          animation: rag-loading-bar 1.1s ease-in-out infinite;
        }
        @keyframes rag-spin { to { transform: rotate(360deg); } }
        @keyframes rag-loading-bar {
          0% { transform: translateX(-120%); opacity: .35; }
          50% { opacity: .8; }
          100% { transform: translateX(260%); opacity: .35; }
        }
        .rag-title, h1, h2, h3, .text-2xl, .text-xl {
          font-family: var(--rag-font-display);
          letter-spacing: 0;
        }
        .rag-title { font-size: clamp(22px, 3.5vw, 34px); font-weight: 760; line-height: 1.05; letter-spacing: 0; }
        .rag-subtitle { color: var(--rag-muted); font-size: 13px; max-width: 820px; }
        .rag-card {
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: var(--rag-shadow);
          backdrop-filter: none;
          transition: box-shadow 0.2s ease;
        }
        .rag-card:hover {
          box-shadow: 0 14px 30px -18px rgba(20, 20, 26, 0.36);
        }
        .rag-search-shell { position: relative; z-index: 5; }
        .rag-search-box {
          background: var(--rag-search-bg);
          border: 1px solid var(--rag-border);
          border-radius: 16px;
          box-shadow: var(--rag-shadow);
          backdrop-filter: blur(12px);
          transition: box-shadow 0.2s ease;
        }
        .rag-search-box:focus-within {
          box-shadow: 0 0 0 2px var(--rag-accent), var(--rag-shadow);
        }
        .rag-ai-expand {
          flex: 0 0 auto;
          min-width: 30px;
          height: 32px;
          padding: 0 2px;
          border: 0;
          border-radius: 0;
          background: transparent;
          opacity: .34;
          text-decoration: line-through;
          text-decoration-thickness: 2px;
          text-decoration-color: currentColor;
          transition: opacity .14s ease, transform .14s ease, outline-color .14s ease;
          transform-origin: center;
        }
        .rag-ai-expand:hover {
          opacity: .72;
          transform: scale(1.06);
        }
        .rag-ai-expand:active {
          transform: scale(.96);
        }
        .rag-ai-expand:focus-within {
          outline: 2px solid color-mix(in srgb, var(--rag-accent) 70%, transparent);
          outline-offset: 3px;
          border-radius: 6px;
        }
        .rag-ai-expand .q-checkbox__label {
          font-weight: 700;
          color: var(--rag-text);
          line-height: 1;
        }
        .rag-ai-expand .q-checkbox__inner {
          display: none;
        }
        .rag-ai-expand[aria-checked="true"],
        .rag-ai-expand.q-checkbox--truthy {
          opacity: 1;
          text-decoration: none;
        }
        .rag-ai-expand[aria-checked="true"]:hover,
        .rag-ai-expand.q-checkbox--truthy:hover {
          opacity: 1;
          transform: scale(1.06);
        }
        .rag-suggest {
          position: absolute;
          left: 0;
          right: 0;
          top: calc(100% + 8px);
          background: var(--rag-suggest-bg);
          border: 1px solid var(--rag-border);
          border-radius: 16px;
          box-shadow: 0 24px 48px -12px rgba(0, 0, 0, 0.18);
          backdrop-filter: blur(16px);
          overflow: hidden;
          z-index: 30;
        }
        .rag-result {
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          padding: 12px;
          box-shadow: 0 4px 14px rgba(0, 0, 0, 0.02);
          width: 100%;
          box-sizing: border-box;
          backdrop-filter: blur(8px);
          transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .rag-result:hover {
          background: var(--rag-surface-strong);
          box-shadow: 0 12px 24px -8px rgba(0, 0, 0, 0.1);
          border-color: rgba(59, 130, 246, 0.3);
        }
        .rag-meta { color: var(--rag-muted); font-size: 12px; }
        .rag-meta mark.rag-highlight {
          background: rgba(250, 204, 21, 0.35);
          color: inherit;
          border-radius: 2px;
          padding: 0 1px;
          font-weight: 600;
        }
        .dark .rag-meta mark.rag-highlight {
          background: rgba(250, 204, 21, 0.2);
        }
        .rag-chip {
          display: inline-flex;
          align-items: center;
          min-height: 28px;
          padding: 0 12px;
          border: 1px solid var(--rag-border);
          border-radius: 14px;
          color: var(--rag-muted);
          background: var(--rag-chip-bg);
          backdrop-filter: blur(4px);
          font-size: 12px;
          font-weight: 500;
          cursor: pointer;
          user-select: none;
          transition: all 0.2s ease;
        }
        .rag-chip:hover {
          background: var(--rag-surface-strong);
          color: var(--rag-accent);
          border-color: var(--rag-accent);
          box-shadow: 0 4px 6px -1px rgba(59, 130, 246, 0.1);
        }
        .rag-chip-active {
          background: linear-gradient(135deg, var(--rag-accent), #2563eb);
          color: #ffffff;
          border-color: transparent;
          font-weight: 600;
          box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
        }
        .rag-chip-active:hover {
          box-shadow: 0 6px 14px rgba(59, 130, 246, 0.4);
        }
        .rag-search-toolbar {
          position: sticky;
          top: 56px;
          z-index: 4;
          padding: 8px;
          border: 1px solid var(--rag-border);
          border-radius: 12px;
          background: color-mix(in srgb, var(--rag-surface) 92%, transparent);
          box-shadow: var(--rag-shadow);
          backdrop-filter: blur(14px);
        }
        .rag-section-label {
          color: var(--rag-muted);
          font-size: 11px;
          font-weight: 700;
          letter-spacing: .08em;
        }
        .rag-explorer-v2-layout {
          display: grid;
          grid-template-columns: minmax(220px, 260px) minmax(0, 1fr) minmax(220px, 280px);
          align-items: stretch;
          height: calc(100vh - 208px);
          min-height: 360px;
          overflow: hidden;
        }
        body:has(.rag-explorer-v2-layout),
        body:has(.rag-explorer-v2-layout) .q-page {
          overflow: hidden;
        }
        .rag-explorer-tree,
        .rag-explorer-details {
          position: static;
          height: 100%;
          min-height: 0;
          overflow: hidden;
        }
        .rag-explorer-files {
          min-width: 0;
          height: 100%;
          min-height: 0;
          overflow-y: auto;
          overscroll-behavior: contain;
        }
        .rag-explorer-mobile-only { display: none !important; }
        .rag-explorer-commandbar {
          padding: 14px 8px 2px;
        }
        .rag-explorer-topline,
        .rag-explorer-actionline {
          display: flex;
          align-items: center;
          width: 100%;
          gap: 10px;
          min-width: 0;
        }
        .rag-explorer-iconbtn {
          width: 32px !important;
          height: 32px !important;
          min-width: 32px !important;
          min-height: 32px !important;
          color: var(--rag-muted) !important;
        }
        .rag-explorer-iconbtn:hover {
          color: var(--rag-text) !important;
          background: var(--rag-sunken) !important;
        }
        .rag-explorer-pathbar,
        .rag-explorer-folder-search {
          min-height: 32px;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          background: var(--rag-surface);
          color: var(--rag-text);
        }
        .rag-explorer-pathbar {
          flex: 1 1 auto;
          min-width: 260px;
          padding: 0 10px;
        }
        .rag-explorer-folder-search {
          flex: 0 0 min(280px, 28vw);
          padding: 0 10px;
        }
        .rag-explorer-folder-search .q-field__control,
        .rag-explorer-folder-search .q-field__native,
        .rag-explorer-folder-search .q-field__append,
        .rag-explorer-folder-search .q-field__prepend {
          min-height: 30px !important;
          height: 30px !important;
        }
        .rag-explorer-folder-search .q-field__control {
          background: transparent !important;
          border: 0 !important;
          box-shadow: none !important;
        }
        .rag-explorer-actionline {
          padding-top: 6px;
          flex-wrap: wrap;
        }
        .rag-explorer-actionline .q-btn {
          min-height: 30px !important;
          height: 30px !important;
          border-radius: 7px !important;
          font-weight: 400 !important;
        }
        .rag-explorer-actionline .q-btn__content {
          font-weight: 400 !important;
        }
        .rag-explorer-actionline .q-field {
          min-width: 120px;
        }
        .rag-explorer-sort-label {
          color: var(--rag-muted);
          font-family: var(--rag-font-mono);
          font-size: 10px;
          font-weight: 700;
          letter-spacing: 0.12em;
          text-transform: uppercase;
        }
        @media (max-width: 1400px) {
          .rag-explorer-v2-layout {
            grid-template-columns: minmax(220px, 260px) minmax(0, 1fr);
          }
          .rag-explorer-details {
            display: none;
          }
        }
        .rag-index-phase {
          padding: 10px;
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
        }
        .rag-index-phase.running {
          border-color: color-mix(in srgb, var(--rag-accent) 48%, var(--rag-border));
        }
        .rag-index-phase.failed,
        .rag-index-phase.cancelled {
          border-color: var(--rag-border);
        }
        .rag-phase-status {
          color: var(--rag-accent);
        }
        .rag-phase-status.completed { color: #16a34a; }
        .rag-phase-status.failed { color: #dc2626; }
        .rag-phase-status.cancelled { color: #f59e0b; }
        .rag-phase-status.idle { color: var(--rag-muted); }
        .rag-index-layout {
          display: grid;
          grid-template-columns: minmax(260px, 360px) minmax(0, 1fr);
          gap: 14px;
          align-items: start;
        }
        .rag-index-control-panel {
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
          padding: 12px;
        }
        .rag-pipeline-row {
          display: grid;
          grid-template-columns: 190px minmax(280px, 1fr) minmax(300px, 420px) 150px;
          gap: 12px;
          align-items: center;
          padding: 12px;
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
        }
        .rag-pipeline-row-card {
          width: 100%;
        }
        .rag-pipeline-row.running {
          border-color: color-mix(in srgb, var(--rag-accent) 55%, var(--rag-border));
        }
        .rag-pipeline-head {
          padding: 0 12px;
          border: 0;
          background: transparent;
          box-shadow: none;
        }
        .rag-pipeline-row.failed,
        .rag-pipeline-row.cancelled {
          border-color: var(--rag-border);
        }
        .rag-status-chip.completed {
          color: #16a34a;
          border-color: color-mix(in srgb, #16a34a 45%, var(--rag-border));
        }
        .rag-status-chip.running {
          color: var(--rag-accent);
          border-color: color-mix(in srgb, var(--rag-accent) 45%, var(--rag-border));
        }
        .rag-status-chip.failed {
          color: #dc2626;
          border-color: color-mix(in srgb, #dc2626 45%, var(--rag-border));
        }
        .rag-status-chip.cancelled {
          color: #f59e0b;
          border-color: color-mix(in srgb, #f59e0b 45%, var(--rag-border));
        }
        .rag-content-coverage {
          border: 1px dashed var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
          padding: 10px 12px;
        }
        .rag-pipeline-actions {
          display: flex;
          gap: 4px;
          justify-content: flex-end;
          flex-wrap: nowrap;
          width: 150px;
          min-width: 150px;
        }
        .rag-progress-stack {
          display: flex;
          flex-direction: column;
          gap: 6px;
        }
        .rag-progress-topline {
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .rag-progressbar { height: 6px; }
        .rag-stage-issue {
          color: #dc2626;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          max-width: 100%;
        }
        .rag-pipeline-row > * {
          min-width: 0;
        }
        .rag-index-config-layout {
          display: grid;
          grid-template-columns: minmax(0, 1.15fr) minmax(280px, .85fr);
          gap: 14px;
          align-items: start;
        }
        .rag-analytics-tabs .q-btn {
          border-radius: 999px;
        }
        .rag-kpi {
          min-width: 180px;
          flex: 1 1 180px;
        }
        .rag-kpi-value {
          font-family: var(--rag-font-display);
          font-size: 24px;
          font-weight: 800;
        }
        .rag-mini-bar {
          height: 5px;
          width: 100%;
          border-radius: 999px;
          background: linear-gradient(90deg, var(--rag-accent), var(--rag-accent-2));
        }
        .rag-path {
          word-break: break-word;
          overflow-wrap: anywhere;
          color: var(--rag-muted);
          font-size: 12px;
        }
        .rag-actions { display: flex; flex-wrap: wrap; gap: 6px; }
        .rag-feedback-btn {
          width: 30px;
          height: 30px;
          min-width: 30px;
        }
        .rag-nav-button { justify-content: flex-start; border-radius: 8px; text-align: left; }
        .rag-nav-button .q-btn__content { justify-content: flex-start; width: 100%; text-align: left; }
        .rag-nav-button .q-icon { margin-right: 10px; }
        .rag-tree-button {
          min-height: 30px;
          height: 30px;
          padding-top: 0;
          padding-bottom: 0;
          box-sizing: border-box;
        }
        .rag-tree-button .q-btn__content {
          display: grid;
          grid-template-columns: 20px minmax(0, 1fr);
          column-gap: 8px;
          align-items: center;
          flex-wrap: nowrap;
          min-width: 0;
          width: 100%;
          height: 30px;
          min-height: 30px;
        }
        .rag-tree-button .q-icon {
          margin-right: 0;
          width: 20px;
          min-width: 20px;
          font-size: 18px !important;
          line-height: 1;
        }
        .rag-tree-button .block {
          min-width: 0;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
          line-height: 1.2;
        }
        .rag-tree-button.active {
          background: color-mix(in srgb, var(--rag-accent) 14%, transparent);
          color: var(--rag-accent) !important;
          font-weight: 700;
        }
        .rag-tree-button.ancestor {
          color: var(--rag-text) !important;
          font-weight: 650;
        }
        .rag-tree-row {
          display: flex;
          align-items: center;
          min-width: 0;
          width: 100%;
          height: 30px;
          border-radius: 8px;
          box-sizing: border-box;
        }
        .rag-tree-row.active {
          background: color-mix(in srgb, var(--rag-accent) 14%, transparent);
        }
        .rag-tree-row.ancestor {
          background: color-mix(in srgb, var(--rag-surface) 55%, transparent);
        }
        .rag-tree-toggle {
          width: 22px !important;
          min-width: 22px !important;
          height: 30px !important;
          min-height: 30px !important;
          padding: 0 !important;
          color: var(--rag-muted) !important;
        }
        .rag-tree-toggle .q-icon {
          margin: 0 !important;
          font-size: 16px !important;
        }
        .rag-tree-label {
          flex: 1 1 auto;
          min-width: 0;
          height: 30px !important;
          min-height: 30px !important;
          padding: 0 8px 0 2px !important;
        }
        .rag-tree-label .q-icon {
          color: #f2b237 !important;
        }
        .rag-tree-row.active .rag-tree-label,
        .rag-tree-row.active .rag-tree-label .q-icon {
          color: var(--rag-accent) !important;
        }
        .rag-breadcrumbs {
          min-width: 0;
          overflow: hidden;
        }
        .rag-breadcrumbs .q-btn {
          min-width: 0;
          max-width: 220px;
        }
        .rag-breadcrumbs .block {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rag-filter-chip.active {
          border-color: var(--rag-accent);
          color: var(--rag-accent);
          background: color-mix(in srgb, var(--rag-accent) 10%, var(--rag-surface));
          font-weight: 700;
          box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--rag-accent) 16%, transparent);
        }
        .rag-dirty-actions {
          position: sticky;
          bottom: 10px;
          z-index: 25;
          display: flex;
          justify-content: center;
          width: 100%;
          pointer-events: none;
        }
        .rag-dirty-actions > * {
          pointer-events: auto;
        }
        .rag-dirty-actions-inner {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          padding: 8px 10px;
          border: 1px solid var(--rag-border);
          border-radius: 999px;
          background: var(--rag-surface-strong);
          box-shadow: var(--rag-shadow);
          backdrop-filter: blur(12px);
        }
        .cd-status-badge {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          padding: 2px 10px 2px 6px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 600;
          line-height: 1.6;
        }
        .cd-status-pending  { background: #fef9c3; color: #854d0e; }
        .cd-status-running  { background: #dbeafe; color: #1d4ed8; }
        .cd-status-done     { background: #dcfce7; color: #166534; }
        .cd-status-error    { background: #fee2e2; color: #991b1b; }
        .cd-status-cancelled{ background: #f3f4f6; color: #6b7280; }
        .dark .cd-status-pending  { background: #422006; color: #fbbf24; }
        .dark .cd-status-running  { background: #1e3a5f; color: #93c5fd; }
        .dark .cd-status-done     { background: #14532d; color: #86efac; }
        .dark .cd-status-error    { background: #450a0a; color: #fca5a5; }
        .dark .cd-status-cancelled{ background: #1f2937; color: #9ca3af; }
        .cd-jobs-card {
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          padding: 10px 12px;
          background: var(--rag-surface);
          transition: box-shadow 0.15s;
        }
        .cd-jobs-card:hover { box-shadow: var(--rag-shadow); }
        .cd-empty-state {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 6px;
          padding: 24px 16px;
          color: var(--rag-muted);
          font-size: 13px;
        }
        /* Cloud Drive drag-drop zone */
        .cd-drop-zone .q-uploader {
          border: 2px dashed var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
          transition: border-color 0.15s, background 0.15s;
        }
        .cd-drop-zone .q-uploader:hover,
        .cd-drop-zone .q-uploader--dnd {
          border-color: var(--rag-accent);
          background: rgba(61, 99, 255, 0.04);
        }
        /* Context-menu hover highlight */
        .q-menu .q-item.text-negative:hover { background: rgba(220, 38, 38, 0.08); }
        .rag-suggest-item {
          min-width: 0;
          overflow: hidden;
        }
        .rag-suggest-item .q-btn__content {
          width: 100%;
          min-width: 0;
          display: flex;
          align-items: center;
          flex-wrap: nowrap;
          gap: 8px;
          overflow: hidden;
        }
        .rag-suggest-item .q-icon {
          flex: 0 0 20px;
          width: 20px;
          min-width: 20px;
        }
        .rag-suggest-item .block {
          display: block;
          flex: 1 1 auto;
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          line-height: 1.25;
          text-align: left;
        }
        .rag-group-panel {
          width: 100%;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          background: var(--rag-group-bg);
          overflow: hidden;
        }
        .rag-file-icon {
          display: inline-flex;
          width: 34px;
          height: 34px;
          flex: 0 0 34px;
        }
        .rag-file-icon svg { width: 34px; height: 34px; display: block; }
        .rag-file-icon.system {
          opacity: .42;
          filter: grayscale(1);
        }
        .rag-file-icon svg { width: 42px; height: 42px; display: block; }
        .rag-explorer-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
          gap: 10px;
        }
        .rag-explorer-grid.medium { grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); }
        .rag-explorer-grid.small {
          grid-template-columns: repeat(auto-fill, minmax(82px, 92px));
          gap: 8px;
        }
        .rag-explorer-item {
          width: 100%;
          min-width: 0;
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          color: var(--rag-text);
          backdrop-filter: blur(8px);
          transition: all 0.2s ease;
        }
        .rag-explorer-item:hover {
          background: var(--rag-surface-strong);
          border-color: rgba(59, 130, 246, 0.3);
          box-shadow: 0 10px 20px -10px rgba(59, 130, 246, 0.15);
        }
        .rag-explorer-item.selected,
        .rag-file-table-row.selected,
        .rag-explorer-item:has(.rag-select-checkbox .q-checkbox__inner--truthy),
        .rag-file-table-row:has(.rag-select-checkbox .q-checkbox__inner--truthy) {
          border-color: color-mix(in srgb, var(--rag-accent) 55%, var(--rag-border));
          background: color-mix(in srgb, var(--rag-accent) 10%, var(--rag-surface));
        }
        .rag-explorer-item.system {
          opacity: .55;
          color: #64748b;
        }
        .rag-explorer-item.system:hover {
          opacity: .78;
          background: #f1f5f9;
          border-color: #cbd5e1;
        }
        .rag-explorer-item { position: relative; }
        .rag-explorer-grid.small .rag-explorer-item {
          min-height: 96px;
          max-height: 106px;
          padding: 6px;
          overflow: hidden;
        }
        .rag-explorer-grid.small .rag-file-icon,
        .rag-explorer-grid.small .rag-file-icon svg {
          width: 34px;
          height: 34px;
          flex-basis: 34px;
        }
        .rag-explorer-grid.small .rag-favorite-star {
          position: absolute;
          top: 2px;
          right: 2px;
          z-index: 2;
          background: var(--rag-surface);
        }
        .rag-explorer-grid.small .rag-explorer-opener {
          width: 100%;
          min-width: 0;
          overflow: hidden;
        }
        .rag-favorite-star {
          opacity: 0;
          color: rgba(0, 0, 0, 0.45);
          transition: opacity .12s ease, color .12s ease, transform .12s ease;
        }
        .rag-explorer-item:hover .rag-favorite-star,
        .rag-favorite-star.active {
          opacity: 1;
        }
        .rag-favorite-star:hover {
          color: #d89b00;
          transform: scale(1.08);
        }
        .rag-favorite-star.active {
          color: #f6b700;
        }
        .rag-tile-star-wrap {
          position: absolute;
          top: 4px;
          right: 4px;
          z-index: 2;
        }
        .rag-tile-select-wrap {
          position: absolute;
          top: 4px;
          left: 4px;
          z-index: 3;
          border-radius: 999px;
          background: color-mix(in srgb, var(--rag-surface) 88%, transparent);
          opacity: 0;
          pointer-events: none;
          transition: opacity .12s ease;
        }
        .rag-explorer-item:hover .rag-tile-select-wrap,
        .rag-explorer-item:has(.rag-select-checkbox .q-checkbox__inner--truthy) .rag-tile-select-wrap {
          opacity: 1;
          pointer-events: auto;
        }
        .rag-select-checkbox {
          flex: 0 0 auto;
        }
        .rag-select-checkbox .q-checkbox__inner {
          font-size: 22px;
          width: 22px;
          min-width: 22px;
          height: 22px;
        }
        .rag-select-checkbox .q-checkbox__bg {
          border-width: 1px;
          border-radius: 4px;
        }
        .rag-explorer-item .rag-select-checkbox,
        .rag-file-table-row .rag-select-checkbox {
          opacity: 0;
          pointer-events: none;
          transition: opacity .12s ease;
        }
        .rag-explorer-item:hover .rag-select-checkbox,
        .rag-file-table-row:hover .rag-select-checkbox,
        .rag-explorer-item:has(.rag-select-checkbox .q-checkbox__inner--truthy) .rag-select-checkbox,
        .rag-file-table-row:has(.rag-select-checkbox .q-checkbox__inner--truthy) .rag-select-checkbox {
          opacity: 1;
          pointer-events: auto;
        }
        .rag-tile-select-wrap .rag-select-checkbox {
          pointer-events: auto;
        }
        .rag-select-page-checkbox {
          opacity: 1 !important;
          pointer-events: auto !important;
        }
        .rag-select-page-checkbox.rag-select-partial .q-checkbox__inner {
          color: var(--rag-accent);
        }
        .rag-select-page-checkbox.rag-select-partial .q-checkbox__bg {
          position: relative;
          background: var(--rag-accent);
          border-color: var(--rag-accent);
        }
        .rag-select-page-checkbox.rag-select-partial .q-checkbox__svg {
          display: none;
        }
        .rag-select-page-checkbox.rag-select-partial .q-checkbox__bg::after {
          content: "";
          position: absolute;
          left: 4px;
          right: 4px;
          top: 50%;
          height: 2px;
          border-radius: 999px;
          background: white;
          transform: translateY(-50%);
        }
        .rag-file-select-icon {
          position: relative;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 40px;
          min-width: 40px;
          height: 38px;
        }
        .rag-file-select-icon.header {
          height: 26px;
          justify-content: flex-start;
        }
        .rag-file-select-overlay {
          position: absolute;
          left: -2px;
          top: -1px;
          z-index: 2;
          border-radius: 5px;
          background: color-mix(in srgb, var(--rag-surface) 72%, transparent);
          box-shadow: 0 0 0 1px color-mix(in srgb, var(--rag-border) 72%, transparent);
          opacity: 0;
          pointer-events: none;
          transition: opacity .12s ease;
        }
        .rag-file-table-row:hover .rag-file-select-overlay,
        .rag-file-table-row:has(.rag-select-checkbox .q-checkbox__inner--truthy) .rag-file-select-overlay {
          opacity: 1;
          pointer-events: auto;
        }
        .rag-file-select-overlay .rag-select-checkbox {
          display: flex;
        }
        .rag-selection-bar {
          position: fixed;
          left: 50%;
          bottom: 18px;
          z-index: 1200;
          width: min(720px, calc(100vw - 32px));
          transform: translateX(-50%);
          min-height: 32px;
          padding: 4px 7px;
          border: 1px solid color-mix(in srgb, var(--rag-accent) 42%, var(--rag-border));
          border-radius: 999px;
          background: color-mix(in srgb, var(--rag-surface) 92%, transparent);
          color: var(--rag-text);
          box-shadow: 0 12px 30px -18px rgba(0,0,0,.55);
          backdrop-filter: blur(14px);
          justify-content: center;
          flex-wrap: nowrap;
        }
        .rag-favorite-star.header {
          opacity: .65;
        }
        .rag-favorite-star.header:hover {
          opacity: 1;
          color: #d89b00;
        }
        .rag-bookmarks {
          display: flex;
          width: 100%;
          gap: 8px;
          overflow-x: auto;
          padding: 8px 0;
          align-items: center;
          flex-wrap: nowrap;
        }
        .rag-bookmark {
          position: relative;
          flex: 0 0 auto;
          width: 220px;
          min-width: 160px;
          height: 42px;
          border: 1px solid var(--rag-border);
          background: var(--rag-bookmark-bg);
          border-radius: 8px;
          overflow: hidden;
          transition: background .12s ease, border-color .12s ease, box-shadow .12s ease;
        }
        .rag-bookmark:hover {
          background: var(--rag-bookmark-hover-bg);
          border-color: #bdd7e9;
        }
        .rag-bookmark-main {
          position: absolute;
          inset: 0 36px 0 0;
          display: flex;
          align-items: center;
          min-width: 0;
        }
        .rag-bookmark:hover .rag-bookmark-main {
          box-shadow: 18px 0 24px rgba(23, 32, 44, 0.12);
        }
        .rag-bookmark-remove {
          position: absolute;
          right: 0;
          top: 0;
          width: 36px;
          height: 100%;
          display: flex;
          align-items: center;
          justify-content: center;
          opacity: 0;
          background: var(--rag-bookmark-remove-bg);
          border-left: 1px solid var(--rag-border);
          color: #7b8794;
          transition: opacity .12s ease, color .12s ease, background .12s ease;
        }
        .rag-bookmark:hover .rag-bookmark-remove {
          opacity: 1;
        }
        .rag-bookmark-remove:hover {
          background: var(--rag-bookmark-remove-hover-bg);
          color: #b42318;
        }
        .rag-bookmark .q-btn {
          min-width: 0;
          width: 100%;
          height: 100%;
          padding-right: 4px;
        }
        .rag-bookmark .q-btn__content {
          min-width: 0;
          flex-wrap: nowrap;
          overflow: hidden;
        }
        .rag-bookmark .block {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rag-bookmark-more {
          flex: 0 0 auto;
          width: 42px;
          height: 42px;
        }
        .rag-context-menu {
          position: fixed;
          z-index: 10000;
          min-width: 220px;
          background: var(--rag-context-bg);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: 0 18px 48px rgba(23, 32, 44, 0.18);
          padding: 6px;
          display: none;
        }
        .rag-context-menu button {
          display: block;
          width: 100%;
          padding: 8px 10px;
          border: 0;
          background: transparent;
          text-align: left;
          border-radius: 8px;
          color: var(--rag-text);
          cursor: pointer;
        }
        .rag-context-menu button:hover { background: #eef6fb; }
        .rag-favorites-dialog-row {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr) auto;
          gap: 8px;
          align-items: center;
          width: 100%;
        }
        .rag-explorer-name {
          width: 100%;
          min-width: 0;
          overflow-wrap: anywhere;
          word-break: break-word;
          line-height: 1.2;
        }
        .rag-explorer-grid.small .rag-explorer-name {
          display: -webkit-box;
          width: 100%;
          max-width: 100%;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
          text-overflow: ellipsis;
          overflow-wrap: anywhere;
          word-break: break-word;
          font-size: 12px;
          line-height: 1.15;
        }
        .rag-explorer-list {
          display: grid;
          grid-template-columns: 1fr;
          gap: 4px;
        }
        .rag-code {
          white-space: pre-wrap;
          word-break: break-word;
          font-family: var(--rag-font-mono);
          font-size: 12px;
          background: var(--rag-code-bg);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          padding: 12px;
        }
        body.body--dark .rag-context-menu button:hover { background: rgba(30, 64, 175, 0.25); }
        body.body--dark .rag-explorer-item.system:hover {
          background: var(--rag-surface-strong);
          border-color: var(--rag-border-strong);
        }
        @media (max-width: 760px) {
          .rag-page { width: calc(100vw - 20px); padding-top: 18px; }
          body:has(.rag-explorer-v2-layout) .q-page > .nicegui-content {
            padding-top: 4px;
            padding-bottom: 0;
          }
          .rag-page:has(.rag-explorer-v2-layout) {
            padding-top: 4px;
            padding-bottom: 0;
            min-height: 0;
            overflow: hidden;
          }
          .rag-page:has(.rag-explorer-v2-layout) > .w-full {
            gap: 8px;
            min-height: 0;
          }
          .rag-title { font-size: 28px; }
          .rag-actions .q-btn { width: auto; }
          .rag-search-box { box-shadow: 0 4px 12px rgba(23, 32, 44, 0.06); }
          .rag-search-header {
            max-width: calc(100vw - 24px);
          }
          .rag-search-presets {
            width: calc(100% - 4px) !important;
            max-width: 100%;
            display: grid !important;
            grid-template-columns: repeat(auto-fit, minmax(88px, 1fr));
            justify-content: stretch;
          }
          .rag-search-presets .q-btn {
            width: 100%;
            min-width: 0;
            max-width: none;
          }
          .rag-search-toolbar { top: 50px; }
          .rag-explorer-commandbar { padding: 6px 0 0; }
          .rag-explorer-topline,
          .rag-explorer-actionline { gap: 6px; }
          .rag-explorer-topline,
          .rag-explorer-actionline {
            flex-wrap: nowrap;
            overflow: hidden;
          }
          .rag-explorer-pathbar {
            order: 0;
            flex: 1 1 auto;
            min-width: 0;
          }
          .rag-explorer-folder-search,
          .rag-explorer-sort-label,
          .rag-explorer-actionline .q-field {
            display: none !important;
          }
          .rag-index-layout,
          .rag-index-config-layout { display: flex; flex-direction: column; }
          .rag-pipeline-row { display: flex; flex-direction: column; align-items: stretch; }
          .rag-pipeline-actions { justify-content: flex-start; flex-wrap: wrap; }
          .rag-explorer-v2-layout {
            display: block;
            height: calc(100dvh - 126px);
            min-height: 0;
            overflow: hidden;
          }
          .rag-explorer-tree,
          .rag-explorer-details { display: none !important; }
          .rag-explorer-files {
            height: 100%;
            width: 100%;
            max-height: none;
            overflow-y: auto;
          }
          .rag-cd-table-stats {
            display: none !important;
          }
          .rag-explorer-mobile-only { display: inline-flex !important; }
          .rag-filter-top-action { display: none !important; }
          .rag-file-table-actions .q-btn:not(:last-child) {
            display: none !important;
          }
          .rag-selection-bar { width: min(520px, calc(100vw - 24px)); bottom: 10px; }
          .cd-drop-zone {
            display: none !important;
          }
          .rag-mobile-panel-dialog {
            width: min(420px, calc(100vw - 24px));
            max-height: 82vh;
          }
          .rag-mobile-panel-body {
            max-height: 66vh;
            overflow-y: auto;
          }
        }

        /* ================================================================
           DESIGN SYSTEM v2 — from hi-fi prototype
           ================================================================ */

        /* === GOOGLE FONTS — Instrument Serif for hero headings === */
        @import url('https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&display=swap');

        /* === HEADER V2: 3-column grid === */
        .rag-header-v2 {
          height: 56px !important; min-height: 56px !important; max-height: 56px !important;
          display: flex !important;
          align-items: center !important;
          overflow: hidden !important;
          padding: 0 16px !important;
          background: color-mix(in srgb, var(--rag-bg) 88%, transparent) !important;
          border-bottom: 1px solid var(--rag-border) !important;
          backdrop-filter: blur(18px) saturate(140%) !important;
          -webkit-backdrop-filter: blur(18px) saturate(140%) !important;
        }
        .rag-header-v2 .nicegui-content {
          width: 100% !important;
          height: 56px !important;
          min-height: 56px !important;
          display: flex !important;
          align-items: center !important;
          padding: 0 !important;
          margin: 0 !important;
        }
        .rag-header-v2 > .q-toolbar {
          height: 56px !important; min-height: 56px !important; padding: 0 !important;
          align-items: center !important;
          overflow: hidden !important;
        }
        .rag-hdr-grid {
          display: grid;
          grid-template-columns: 220px minmax(0, 1fr) auto;
          align-items: center;
          width: 100%; height: 100%;
          gap: 0;
        }
        .rag-hdr-brand {
          display: flex; align-items: center; gap: 10px; min-width: 0; height: 56px;
        }
        .rag-hdr-brand-name {
          font-family: var(--rag-font-display); font-weight: 700;
          font-size: 14px; letter-spacing: -0.02em; line-height: 1;
          color: var(--rag-text);
          white-space: nowrap;
        }
        .rag-version-chip {
          min-height: 20px;
          padding: 0 7px !important;
          border-radius: 4px;
          font-size: 10px !important;
          line-height: 18px;
          color: var(--rag-text);
          background: transparent;
        }
        .rag-hdr-nav {
          display: flex; gap: 4px; align-items: center; justify-content: center; height: 56px;
        }
        .rag-hdr-center {
          display: flex;
          align-items: center;
          justify-content: center;
          min-width: 0;
          height: 56px;
          overflow: hidden;
        }
        .rag-hdr-center:has(.rag-header-breadcrumbs:not(:empty)) .rag-hdr-nav {
          display: none;
        }
        .rag-header-breadcrumbs {
          flex: 1 1 auto;
          max-width: 100%;
          overflow: hidden;
          justify-content: flex-start;
          padding: 0 10px;
          color: var(--rag-text);
        }
        .rag-header-breadcrumbs .q-btn {
          max-width: min(220px, 34vw);
        }
        .rag-header-breadcrumbs .q-btn__content,
        .rag-header-breadcrumbs .block {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rag-hdr-actions {
          display: flex; gap: 8px; align-items: center; justify-content: flex-end; height: 56px;
          min-width: 0;
        }
        .rag-header-status {
          min-height: 24px;
          padding: 0 12px;
          border-radius: 999px;
          font-family: var(--rag-font-mono);
          color: var(--rag-muted);
          background: var(--rag-sunken);
          border-color: var(--rag-border-strong);
          white-space: nowrap;
        }
        .rag-header-v2 .q-btn,
        .rag-header-v2 .rag-header-button {
          height: 34px !important;
          min-height: 34px !important;
          max-height: 34px !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
          align-self: center !important;
          color: var(--rag-text) !important;
        }
        .rag-header-v2 .q-btn--round {
          width: 34px !important;
          min-width: 34px !important;
        }
        .rag-header-v2 .q-btn__content {
          min-height: 0 !important;
          height: 34px !important;
          line-height: 1 !important;
          align-items: center !important;
          justify-content: center !important;
          flex-wrap: nowrap !important;
        }
        .rag-header-v2 .q-icon {
          font-size: 20px !important;
          line-height: 1 !important;
        }
        .rag-header-v2 .q-img,
        .rag-header-v2 img {
          display: block;
          flex: 0 0 auto;
        }
        .rag-mobile-menu-button {
          display: none !important;
        }
        @media (max-width: 1100px) {
          .rag-mobile-menu-button {
            display: inline-flex !important;
          }
        }
        @media (max-width: 900px) {
          .rag-hdr-grid {
            grid-template-columns: auto 1fr auto;
            column-gap: 8px;
          }
          .rag-hdr-nav { display: none; }
          .rag-header-breadcrumbs {
            padding: 0 6px 0 10px;
          }
          .rag-header-breadcrumbs .q-btn {
            max-width: 36vw;
          }
          .rag-hdr-brand-name,
          .rag-version-chip,
          .rag-header-status { display: none !important; }
        }

        /* === NAV TABS === */
        .rag-nav-tab {
          position: relative;
          padding: 7px 13px; border-radius: 6px;
          font-size: 13px; font-weight: 500; color: var(--rag-muted) !important;
          cursor: pointer; user-select: none;
          display: inline-flex; align-items: center; gap: 7px;
          transition: color 160ms ease, background 160ms ease;
          border: none; background: transparent; line-height: 1;
          text-decoration: none;
        }
        .rag-nav-tab .q-icon,
        .rag-nav-tab .block {
          color: inherit !important;
        }
        .rag-nav-tab:hover { color: var(--rag-text) !important; background: var(--rag-sunken); }
        .rag-nav-tab.active { color: var(--rag-text) !important; font-weight: 600; }
        body.body--dark .rag-nav-tab { color: #7b7b86 !important; }
        body.body--dark .rag-nav-tab:hover,
        body.body--dark .rag-nav-tab.active { color: #f4f4f7 !important; }
        .rag-nav-tab.active::after {
          content: ''; position: absolute;
          left: 13px; right: 13px; bottom: -8px;
          height: 2px; background: var(--rag-accent);
          border-radius: 2px 2px 0 0;
          box-shadow: 0 0 10px color-mix(in srgb, var(--rag-accent) 55%, transparent);
        }
        .rag-nav-tab .q-icon { font-size: 16px !important; }

        /* === STATUS DOTS === */
        .rag-dot {
          display: inline-block; flex-shrink: 0;
          width: 7px; height: 7px; border-radius: 50%;
        }
        .rag-dot.ok {
          background: #16a34a;
          box-shadow: 0 0 0 3px color-mix(in srgb, #16a34a 20%, transparent);
          animation: rag-dot-ok 2.4s ease-in-out infinite;
        }
        .rag-dot.info {
          background: var(--rag-accent);
          box-shadow: 0 0 0 3px color-mix(in srgb, var(--rag-accent) 20%, transparent);
          animation: rag-dot-info 2.4s ease-in-out infinite;
        }
        .rag-dot.warn { background: #f59e0b; }
        .rag-dot.err  { background: #dc2626; }
        @keyframes rag-dot-ok {
          0%, 100% { box-shadow: 0 0 0 2px color-mix(in srgb, #16a34a 25%, transparent); }
          50%       { box-shadow: 0 0 0 5px color-mix(in srgb, #16a34a 10%, transparent); }
        }
        @keyframes rag-dot-info {
          0%, 100% { box-shadow: 0 0 0 2px color-mix(in srgb, var(--rag-accent) 25%, transparent); }
          50%       { box-shadow: 0 0 0 5px color-mix(in srgb, var(--rag-accent) 10%, transparent); }
        }

        /* === TICKER (animated live dot) === */
        .rag-ticker-dot {
          display: inline-block; width: 5px; height: 5px; border-radius: 50%;
          background: #22d3ee; box-shadow: 0 0 8px #22d3ee;
          animation: rag-ticker 1.2s ease-in-out infinite alternate;
          flex-shrink: 0;
        }
        @keyframes rag-ticker {
          from { opacity: 0.3; transform: scale(0.8); }
          to   { opacity: 1;   transform: scale(1.2); }
        }

        /* === MONO LABEL === */
        .rag-mono-label {
          font-family: var(--rag-font-mono); font-size: 10px; font-weight: 500;
          text-transform: uppercase; letter-spacing: 0.12em; color: var(--rag-muted);
        }

        /* === KBD HINT === */
        .rag-kbd {
          display: inline-flex; align-items: center;
          padding: 1px 6px; border-radius: 4px;
          border: 1px solid var(--rag-border-strong);
          background: var(--rag-sunken);
          font-family: var(--rag-font-mono); font-size: 10px; font-weight: 500;
          color: var(--rag-muted); line-height: 1.6;
        }

        /* === BUTTON SYSTEM === */
        .rag-btn {
          display: inline-flex; align-items: center; gap: 7px;
          padding: 7px 14px; border-radius: 8px;
          font-family: var(--rag-font-display); font-size: 13px; font-weight: 600;
          cursor: pointer; user-select: none; white-space: nowrap;
          border: 1px solid transparent;
          transition: transform 120ms ease, box-shadow 160ms ease, background 160ms ease, color 160ms ease;
          line-height: 1;
          text-decoration: none;
        }
        .rag-btn:active { transform: scale(0.97); }
        .rag-btn-sm { padding: 5px 10px; font-size: 12px; border-radius: 6px; }
        .rag-btn-icon { padding: 7px; border-radius: 8px; }
        .rag-btn-primary {
          background: var(--rag-accent); color: white; border-color: var(--rag-accent);
        }
        .rag-btn-primary:hover {
          background: var(--rag-accent-hover);
          box-shadow: 0 8px 20px -6px color-mix(in srgb, var(--rag-accent) 55%, transparent);
        }
        .rag-btn-secondary {
          background: var(--rag-surface); color: var(--rag-text); border-color: var(--rag-border);
        }
        .rag-btn-secondary:hover { background: var(--rag-sunken); border-color: var(--rag-border-strong); }
        .rag-btn-ghost {
          background: transparent; color: var(--rag-muted); border-color: transparent;
        }
        .rag-btn-ghost:hover { background: var(--rag-sunken); color: var(--rag-text); }
        .rag-btn-danger {
          background: transparent; color: var(--rag-danger); border-color: var(--rag-danger);
        }
        .rag-btn-danger:hover { background: color-mix(in srgb, var(--rag-danger) 8%, transparent); }

        /* === CARD (design-system card, augments rag-card) === */
        .ds-card {
          background: var(--rag-surface); border: 1px solid var(--rag-border);
          border-radius: 12px; padding: 20px;
          box-shadow: 0 1px 0 rgba(20,20,26,.04), 0 1px 3px rgba(20,20,26,.05);
        }

        /* === INPUT === */
        .rag-input {
          display: flex; align-items: center; gap: 8px;
          padding: 8px 12px; border-radius: 8px;
          border: 1px solid var(--rag-border); background: var(--rag-surface);
          font-family: var(--rag-font-text); font-size: 13px; color: var(--rag-text);
          transition: border-color 160ms, box-shadow 160ms;
        }
        .rag-input:focus-within {
          border-color: var(--rag-accent);
          box-shadow: 0 0 0 3px color-mix(in srgb, var(--rag-accent) 15%, transparent);
        }
        .rag-input input {
          flex: 1; border: none; outline: none; background: transparent;
          font-family: inherit; font-size: inherit; color: inherit;
        }

        /* === FILE ICON BOXES === */
        .rag-file-badge {
          display: inline-flex; align-items: center; justify-content: center;
          width: 36px; height: 36px; border-radius: 8px; flex-shrink: 0;
          font-family: var(--rag-font-mono); font-size: 9px; font-weight: 700;
          letter-spacing: 0.04em; text-transform: uppercase;
        }
        .rag-file-badge.pdf { background: #fee2e2; color: #dc2626; }
        .rag-file-badge.doc { background: #dbeafe; color: #2563eb; }
        .rag-file-badge.xls { background: #dcfce7; color: #16a34a; }
        .rag-file-badge.img { background: #fce7f3; color: #db2777; }
        .rag-file-badge.txt { background: var(--rag-sunken); color: var(--rag-muted); }
        .rag-file-badge.fld { background: #fef3c7; color: #92400e; }
        .rag-file-badge .rag-folder-badge-icon {
          font-size: 21px;
          line-height: 1;
        }
        body.body--dark .rag-file-badge.pdf { background: #450a0a; color: #fca5a5; }
        body.body--dark .rag-file-badge.doc { background: #1e3a5f; color: #93c5fd; }
        body.body--dark .rag-file-badge.xls { background: #14532d; color: #86efac; }
        body.body--dark .rag-file-badge.img { background: #500724; color: #f9a8d4; }
        body.body--dark .rag-file-badge.fld { background: #422006; color: #fbbf24; }
        .rag-file-badge.ppt { background: #ffedd5; color: #c2410c; }
        body.body--dark .rag-file-badge.ppt { background: #431407; color: #fb923c; }

        /* === SETTINGS NAV === */
        .rag-settings-nav-item {
          color: var(--rag-text);
          border-radius: 6px;
          transition: background 0.12s, color 0.12s;
        }
        .rag-settings-nav-item .q-icon { color: var(--rag-muted); transition: color 0.12s; }
        .rag-settings-nav-item:hover { background: var(--rag-hover); }
        .rag-settings-nav-item:hover .q-icon { color: var(--rag-text); }
        .rag-settings-nav-item.active {
          background: color-mix(in srgb, var(--rag-accent) 12%, transparent);
          color: var(--rag-accent);
          font-weight: 600;
        }
        .rag-settings-nav-item.active .q-icon { color: var(--rag-accent); }
        .rag-settings-section-label {
          font-size: 10px;
          font-weight: 700;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: var(--rag-muted-2);
          padding: 12px 8px 4px;
        }

        /* === FILE TABLE (cloud drive Таблица view) === */
        .rag-file-table-header,
        .rag-file-table-row {
          display: grid;
          grid-template-columns: 40px minmax(220px,1fr) 120px 88px 84px 74px 88px;
          align-items: center;
          gap: 7px;
          width: 100%;
          box-sizing: border-box;
          padding: 4px 8px;
        }
        .rag-file-table-header {
          border-bottom: 1px solid var(--rag-border);
          padding-bottom: 6px;
          margin-bottom: 2px;
        }
        .rag-file-table-row {
          min-height: 38px;
          border-radius: 5px;
          border-bottom: 1px solid color-mix(in srgb, var(--rag-border) 54%, transparent);
          transition: background 0.1s;
        }
        .rag-file-table-row:hover { background: var(--rag-hover); }
        .rag-hidden-item {
          opacity: .46;
        }
        .rag-hidden-item:hover {
          opacity: .72;
        }
        .rag-context-action-hidden {
          display: none !important;
        }
        .rag-file-table-name .q-btn {
          width: 100%;
          min-width: 0;
        }
        .rag-file-table-name .q-btn__content {
          justify-content: flex-start !important;
          min-width: 0;
          overflow: hidden;
        }
        .rag-file-table-name .block {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rag-file-table-actions {
          display: flex;
          align-items: center;
          justify-content: flex-end;
          gap: 2px;
          min-width: 0;
        }
        .rag-file-table-actions .q-btn {
          width: 28px !important;
          height: 28px !important;
          min-width: 28px !important;
          min-height: 28px !important;
        }
        .rag-file-table-index-ok {
          color: #16a34a;
          font-family: var(--rag-font-mono);
          font-weight: 700;
        }
        .rag-col-header {
          font-size: 11px;
          font-weight: 600;
          color: var(--rag-muted);
          text-transform: uppercase;
          letter-spacing: 0.06em;
          white-space: nowrap;
        }
        .rag-cd-mobile-count,
        .rag-cd-mobile-badge {
          display: none !important;
        }
        .rag-file-table-head-name,
        .rag-file-table-head-size {
          min-width: 0;
        }
        @media (max-width: 760px) {
          .rag-file-table-header,
          .rag-file-table-row {
            grid-template-columns: 40px minmax(0,1fr) 104px;
            min-width: 0;
          }
          .rag-file-table-header > :nth-child(3),
          .rag-file-table-header > :nth-child(5),
          .rag-file-table-header > :nth-child(6),
          .rag-file-table-header > :nth-child(7),
          .rag-file-table-row > :nth-child(3),
          .rag-file-table-row > :nth-child(5),
          .rag-file-table-row > :nth-child(6),
          .rag-file-table-row > :nth-child(7) {
            display: none !important;
          }
          .rag-file-table-header {
            min-height: 38px;
          }
          .rag-col-name-title,
          .rag-col-size-title {
            display: none !important;
          }
          .rag-cd-mobile-count {
            display: block !important;
            min-width: 0;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            font-size: 12px;
          }
          .rag-cd-mobile-badge {
            display: inline-flex !important;
            max-width: 100%;
            justify-content: center;
            white-space: nowrap;
          }
        }

        /* === SHIMMER ANIMATION === */
        @keyframes rag-shimmer {
          0%   { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }
        .rag-shimmer {
          background: linear-gradient(90deg, var(--rag-sunken) 25%, var(--rag-border) 50%, var(--rag-sunken) 75%);
          background-size: 200% 100%;
          animation: rag-shimmer 1.4s linear infinite;
          border-radius: 4px;
        }

        /* === PROGRESS BAR === */
        .rag-progress-bar {
          height: 6px; border-radius: 999px;
          background: var(--rag-border); overflow: hidden;
        }
        .rag-progress-bar > div {
          height: 100%; border-radius: 999px;
          background: var(--rag-accent);
          transition: width 400ms ease;
        }

        /* === GLASS PANEL === */
        .rag-glass {
          background: color-mix(in srgb, var(--rag-surface) 80%, transparent);
          border: 1px solid var(--rag-border);
          backdrop-filter: blur(12px) saturate(130%);
          -webkit-backdrop-filter: blur(12px) saturate(130%);
          border-radius: 10px;
        }

        /* === DIVIDER WITH TEXT === */
        .rag-divider-text {
          display: flex; align-items: center; gap: 12px;
          color: var(--rag-muted); font-family: var(--rag-font-mono);
          font-size: 11px; text-transform: uppercase; letter-spacing: 0.1em;
        }
        .rag-divider-text::before, .rag-divider-text::after {
          content: ''; flex: 1; height: 1px; background: var(--rag-border);
        }

        /* === USER AVATAR CHIP === */
        .rag-avatar {
          width: 30px; height: 30px; border-radius: 50%; flex-shrink: 0;
          background: linear-gradient(135deg, var(--rag-accent), #22d3ee);
          color: white; display: grid; place-items: center;
          font-family: var(--rag-font-display); font-size: 12px; font-weight: 700;
          cursor: pointer; user-select: none;
        }

        /* === LOGIN SPLIT LAYOUT === */
        .rag-login-split {
          display: grid; grid-template-columns: 1.2fr 1fr;
          height: 100%; overflow: hidden;
        }
        .rag-login-brand {
          position: relative; overflow: hidden;
          background: #08080c; color: #fff;
          padding: 40px 56px;
          display: flex; flex-direction: column;
        }
        .rag-login-brand-liquid {
          position: absolute; inset: 0; pointer-events: none; z-index: 0;
          background:
            radial-gradient(ellipse 60% 50% at 20% 30%, color-mix(in srgb, #3d63ff 25%, transparent), transparent),
            radial-gradient(ellipse 50% 40% at 80% 70%, color-mix(in srgb, #22d3ee 18%, transparent), transparent);
        }
        .rag-login-brand-grid {
          position: absolute; inset: 0; pointer-events: none; z-index: 0;
          background-image:
            linear-gradient(rgba(255,255,255,.04) 1px, transparent 1px),
            linear-gradient(90deg, rgba(255,255,255,.04) 1px, transparent 1px);
          background-size: 40px 40px;
          mask-image: radial-gradient(ellipse 80% 80% at 50% 50%, black 30%, transparent 100%);
        }
        .rag-login-brand > * { position: relative; z-index: 1; }
        .rag-login-form-panel {
          display: flex; flex-direction: column;
          padding: 24px 32px;
          background: var(--rag-bg);
        }
        .rag-login-stat-block {
          display: flex; flex-direction: column; gap: 4px;
          padding: 0 24px;
          border-left: 1px solid rgba(255,255,255,.1);
        }
        .rag-login-stat-block:first-child { padding-left: 0; border-left: none; }
        .rag-login-stat-num {
          font-family: var(--rag-font-display); font-weight: 800;
          font-size: 32px; letter-spacing: -0.03em; line-height: 1;
          color: #fff; font-variant-numeric: tabular-nums;
        }
        .rag-login-activity-row {
          display: grid; grid-template-columns: 44px 120px 1fr;
          gap: 10px; padding: 5px 0;
          font-family: var(--rag-font-mono); font-size: 11px;
          color: rgba(255,255,255,.45);
          border-top: 1px solid rgba(255,255,255,.06);
        }
        .rag-login-activity-row:first-child { border-top: none; }
        @media (max-width: 768px) {
          .rag-login-split { grid-template-columns: 1fr; }
          .rag-login-brand { display: none; }
          .rag-login-form-panel { padding: 24px 20px; }
        }

        /* ── FRESHNESS BAR (под прогрессом этапа индекса) ───────────── */
        .rag-freshness {
          display: flex; flex-direction: column; gap: 4px;
          margin-top: 6px;
        }
        .rag-freshness-bar {
          height: 3px; border-radius: 2px;
          background: var(--rag-border);
        }
        .rag-freshness-bar.fresh   { background: #16a34a; box-shadow: 0 0 8px rgba(22,163,74,.4); }
        .rag-freshness-bar.running { background: var(--rag-accent); box-shadow: 0 0 8px color-mix(in srgb, var(--rag-accent) 50%, transparent); }
        .rag-freshness-bar.stale   { background: #dc2626; box-shadow: 0 0 8px rgba(220,38,38,.4); }
        .rag-freshness-bar.empty   { background: var(--rag-border); }
        .rag-freshness-label {
          font-family: var(--rag-font-mono); font-size: 10px;
          color: var(--rag-muted-2); letter-spacing: 0.06em;
          text-transform: uppercase; line-height: 1;
          display: flex; align-items: center; gap: 6px;
        }
        .rag-freshness-label .age { color: var(--rag-text); font-weight: 600; }
        .rag-freshness-label .age.fresh   { color: #16a34a; }
        .rag-freshness-label .age.running { color: var(--rag-accent); }
        .rag-freshness-label .age.stale   { color: #dc2626; }

        /* ── GLOW-CARD (для AI-ответа, hero-блоков) ─────────────────── */
        .rag-glow-card {
          position: relative; isolation: isolate;
          background: linear-gradient(135deg, color-mix(in srgb, var(--rag-accent) 5%, var(--rag-surface)), var(--rag-surface) 60%);
          border: 1px solid var(--rag-border);
          border-radius: 12px;
          padding: 18px 22px;
          overflow: hidden;
        }
        .rag-glow-card::before {
          content: ''; position: absolute; inset: -1px;
          border-radius: 12px; padding: 1px;
          background: linear-gradient(135deg,
            color-mix(in srgb, var(--rag-accent) 35%, transparent),
            transparent 40%,
            color-mix(in srgb, #22d3ee 25%, transparent));
          -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
          -webkit-mask-composite: xor; mask-composite: exclude;
          pointer-events: none;
        }
        .rag-glow-card::after {
          content: ''; position: absolute; top: 0; right: 0;
          width: 220px; height: 100px;
          background: radial-gradient(circle at 70% 30%, color-mix(in srgb, var(--rag-accent) 18%, transparent), transparent 70%);
          pointer-events: none; z-index: 0;
        }
        .rag-glow-card > * { position: relative; z-index: 1; }
        .rag-ai-badge {
          display: inline-flex; align-items: center; justify-content: center;
          width: 28px; height: 28px; border-radius: 8px;
          background: linear-gradient(135deg, var(--rag-accent), #22d3ee);
          color: #fff; flex-shrink: 0;
          box-shadow: 0 4px 16px -4px color-mix(in srgb, var(--rag-accent) 50%, transparent);
        }
        .rag-ai-meta {
          font-family: var(--rag-font-mono); font-size: 10px;
          text-transform: uppercase; letter-spacing: 0.1em;
          color: var(--rag-muted);
        }
        .rag-ai-title {
          font-family: var(--rag-font-display);
          font-weight: 700; font-size: 14px; line-height: 1;
        }

        /* ── RIGHT-SIDE PREVIEW DRAWER ───────────────────────────────── */
        .rag-preview-drawer {
          position: fixed; top: 56px; right: 0; bottom: 0;
          width: 520px; max-width: 92vw;
          background: var(--rag-surface);
          border-left: 1px solid var(--rag-border);
          box-shadow: -24px 0 48px -24px rgba(0,0,0,.18);
          transform: translateX(0);
          transition: transform 320ms cubic-bezier(.65,0,.35,1);
          z-index: 1500;
          display: flex; flex-direction: column;
          overflow: hidden;
        }
        .rag-preview-drawer.closed { transform: translateX(110%); }
        .rag-preview-drawer-scrim {
          position: fixed; inset: 56px 0 0 0;
          background: rgba(20,20,26,.42);
          z-index: 1499;
          opacity: 1; transition: opacity 240ms;
        }
        .rag-preview-drawer-scrim.closed { opacity: 0; pointer-events: none; }
        .rag-preview-drawer-header {
          flex-shrink: 0; padding: 14px 18px;
          border-bottom: 1px solid var(--rag-border);
          display: flex; align-items: center; gap: 12px;
        }
        .rag-preview-drawer-tabs {
          flex-shrink: 0;
          display: flex; gap: 0;
          border-bottom: 1px solid var(--rag-border);
          padding: 0 14px;
        }
        .rag-preview-drawer-tab {
          position: relative; padding: 12px 14px;
          border: none; background: transparent; cursor: pointer;
          font-size: 13px; font-weight: 500;
          color: var(--rag-muted);
        }
        .rag-preview-drawer-tab.active { color: var(--rag-text); }
        .rag-preview-drawer-tab.active::after {
          content: ''; position: absolute; bottom: -1px; left: 12px; right: 12px;
          height: 2px; background: var(--rag-accent);
          box-shadow: 0 0 10px color-mix(in srgb, var(--rag-accent) 50%, transparent);
        }
        .rag-preview-drawer-body {
          flex: 1; overflow: auto; padding: 20px;
        }
        .rag-preview-drawer-actions {
          flex-shrink: 0; padding: 14px 18px;
          border-top: 1px solid var(--rag-border);
          display: flex; gap: 6px; flex-wrap: wrap;
        }

        /* ── CLOUD DRIVE styles ──────────────────────────────────────── */
        .rag-cloud-hero {
          display: flex; flex-direction: column; align-items: center;
          text-align: center; padding: 56px 24px 40px;
          max-width: 720px; margin: 0 auto;
        }
        .rag-cloud-hero-icon {
          width: 80px; height: 80px; border-radius: 20px;
          background: linear-gradient(135deg, var(--rag-accent), #22d3ee);
          display: grid; place-items: center; color: white;
          box-shadow: 0 24px 48px -12px color-mix(in srgb, var(--rag-accent) 50%, transparent);
          margin-bottom: 24px;
        }
        .rag-cloud-hero-title {
          font-family: var(--rag-font-display); font-weight: 800;
          font-size: 32px; letter-spacing: -0.03em; margin: 0 0 8px;
        }
        .rag-cloud-hero-subtitle {
          color: var(--rag-muted); font-size: 15px; max-width: 540px;
          line-height: 1.55; margin: 0;
        }
        .rag-cloud-action-grid {
          display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 12px; max-width: 960px; margin: 24px auto 0;
        }
        .rag-cloud-action-card {
          display: flex; flex-direction: column; gap: 12px;
          padding: 20px; border-radius: 10px;
          background: var(--rag-surface); border: 1px solid var(--rag-border);
          cursor: pointer; transition: all 200ms;
        }
        .rag-cloud-action-card:hover {
          transform: translateY(-2px);
          border-color: color-mix(in srgb, var(--rag-accent) 30%, var(--rag-border));
          box-shadow: var(--rag-shadow);
        }
        .rag-cloud-action-card.featured {
          background: linear-gradient(135deg, color-mix(in srgb, var(--rag-accent) 6%, var(--rag-surface)), var(--rag-surface));
        }
        .rag-cloud-action-card-icon {
          width: 44px; height: 44px; border-radius: 10px;
          display: grid; place-items: center;
          background: var(--rag-sunken); color: var(--rag-text);
        }
        .rag-cloud-action-card.featured .rag-cloud-action-card-icon {
          background: linear-gradient(135deg, var(--rag-accent), #22d3ee); color: white;
        }
        .rag-cloud-action-card-title { font-family: var(--rag-font-display); font-weight: 700; font-size: 15px; }
        .rag-cloud-action-card-desc  { color: var(--rag-muted); font-size: 12px; line-height: 1.4; }
        .rag-cloud-action-card-cta {
          display: flex; align-items: center; gap: 6px;
          margin-top: auto; color: var(--rag-muted-2);
          font-size: 12px; font-weight: 600;
        }
        .rag-cloud-action-card.featured .rag-cloud-action-card-cta { color: var(--rag-accent); }
        .rag-cloud-drop-zone {
          margin: 24px auto 0; max-width: 960px; padding: 32px;
          border: 2px dashed var(--rag-border-strong); border-radius: 12px;
          text-align: center; color: var(--rag-muted); background: var(--rag-sunken);
        }
        .rag-cloud-drop-zone-title { font-size: 14px; font-weight: 500; color: var(--rag-text); }
        .rag-cloud-drop-zone-meta {
          font-family: var(--rag-font-mono); font-size: 11px;
          color: var(--rag-muted-2); margin-top: 6px;
          text-transform: uppercase; letter-spacing: 0.08em;
        }
        .rag-cloud-kpi-grid {
          display: grid; grid-template-columns: repeat(4, 1fr);
          gap: 12px; margin-bottom: 24px;
        }
        .rag-cloud-kpi {
          background: var(--rag-surface); border: 1px solid var(--rag-border);
          border-radius: 10px; padding: 16px;
        }
        .rag-cloud-kpi-label {
          font-family: var(--rag-font-mono); font-size: 10px;
          text-transform: uppercase; letter-spacing: 0.1em;
          color: var(--rag-muted); margin-bottom: 8px;
        }
        .rag-cloud-kpi-value {
          font-family: var(--rag-font-display); font-weight: 800;
          font-size: 26px; letter-spacing: -0.02em; line-height: 1;
        }
        .rag-cloud-kpi-sub {
          font-family: var(--rag-font-mono); font-size: 10px;
          color: var(--rag-muted-2); text-transform: uppercase;
          letter-spacing: 0.08em; margin-top: 6px;
        }
        .rag-cloud-job-row {
          display: grid; grid-template-columns: 40px minmax(0,1fr) 160px 110px;
          gap: 14px; align-items: center; padding: 12px 16px;
          background: var(--rag-surface); border: 1px solid var(--rag-border);
          border-radius: 10px;
        }
        .rag-cloud-job-row.running {
          border-color: color-mix(in srgb, var(--rag-accent) 40%, var(--rag-border));
        }
        .rag-cloud-job-name {
          font-size: 13px; font-weight: 600;
          white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }
        .rag-cloud-job-meta {
          font-family: var(--rag-font-mono); font-size: 10px;
          color: var(--rag-muted); margin-top: 2px;
          text-transform: uppercase; letter-spacing: 0.08em;
        }
        </style>""")
    _INTERACTION_SCRIPT_CACHE = INTERACTION_JS_PATH.read_text(encoding="utf-8") if INTERACTION_JS_PATH.exists() else ""
    ui.add_body_html(
        """
        <div id="rag-global-busy" class="rag-global-busy" role="status" aria-live="polite">
          <span class="rag-busy-spinner" aria-hidden="true"></span>
          <span class="rag-busy-content">
            <span class="rag-busy-label">Выполняется...</span>
            <span class="rag-busy-skeleton" aria-hidden="true"></span>
          </span>
        </div>
        <div id="rag-global-context-menu" class="rag-context-menu" role="menu"></div>
        """
    )
