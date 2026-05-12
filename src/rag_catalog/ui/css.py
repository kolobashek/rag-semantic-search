"""
css.py — Global CSS and JS injection for the NiceGUI app.

Call _install_css() once inside a @ui.page handler before rendering content.
"""

from __future__ import annotations

from nicegui import ui


def _install_css() -> None:
    ui.add_head_html('<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">')
    ui.add_head_html('<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">')
    ui.add_css(
        """
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
        .rag-page.search { padding-top: 4px; }
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
          grid-template-columns: minmax(190px, 240px) minmax(0, 1fr) minmax(210px, 270px);
        }
        .rag-explorer-tree,
        .rag-explorer-details {
          position: sticky;
          top: 66px;
          max-height: calc(100vh - 84px);
          overflow: auto;
        }
        .rag-explorer-files {
          min-width: 0;
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
          min-height: 34px;
          height: auto;
          padding-top: 4px;
          padding-bottom: 4px;
        }
        .rag-tree-button .q-btn__content {
          display: grid;
          grid-template-columns: 22px minmax(0, 1fr);
          column-gap: 8px;
          align-items: start;
          flex-wrap: nowrap;
          min-width: 0;
          width: 100%;
        }
        .rag-tree-button .q-icon {
          margin-right: 0;
          width: 22px;
          min-width: 22px;
          line-height: 1.25;
        }
        .rag-tree-button .block {
          min-width: 0;
          white-space: normal;
          overflow-wrap: anywhere;
          word-break: normal;
          line-height: 1.25;
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
          border-radius: 12px;
          color: var(--rag-text);
          backdrop-filter: blur(8px);
          transition: all 0.2s ease;
        }
        .rag-explorer-item:hover {
          background: var(--rag-surface-strong);
          border-color: rgba(59, 130, 246, 0.3);
          box-shadow: 0 10px 20px -10px rgba(59, 130, 246, 0.15);
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
          .rag-title { font-size: 28px; }
          .rag-actions .q-btn { width: auto; }
          .rag-search-box { box-shadow: 0 4px 12px rgba(23, 32, 44, 0.06); }
          .rag-search-toolbar { top: 50px; }
          .rag-index-layout,
          .rag-index-config-layout { display: flex; flex-direction: column; }
          .rag-pipeline-row { display: flex; flex-direction: column; align-items: stretch; }
          .rag-pipeline-actions { justify-content: flex-start; flex-wrap: wrap; }
          .rag-explorer-v2-layout { display: flex; flex-direction: column; }
          .rag-explorer-tree,
          .rag-explorer-details {
            position: static;
            max-height: none;
            width: 100%;
          }
        }
        """
    )
    ui.add_body_html(
        """
        <div id="rag-global-context-menu" class="rag-context-menu" role="menu"></div>
        <script>
        (() => {
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
                  const a = document.createElement('a');
                  a.href = itemUrl;
                  a.download = '';
                  document.body.appendChild(a);
                  a.click();
                  a.remove();
                });
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
          document.addEventListener('contextmenu', show);
          document.addEventListener('click', hide);
          document.addEventListener('scroll', hide, true);
          document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });
        })();
        </script>
        """
    )
