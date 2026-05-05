# Mobile Modernization — Implementation Handoff

A surgical guide for applying the **Hearth & Parchment mobile redesign** to `frontend/index.html`. All changes are **additive and mobile-scoped** — desktop behavior is untouched.

> Reference: `Church History App - Mobile Modernization.html` (5 screens, light + dark) and `mobile-hearth.jsx` (component sketches).

---

## 0 · Strategy

| Concern | Decision |
|---|---|
| **Scope of changes** | Mobile only (`@media (max-width: 700px)`). Desktop unchanged. |
| **Where new CSS lives** | One new `<style>` block at the end of the existing `<style>` tag, behind the mobile breakpoint. |
| **Where new JS lives** | Two new IIFEs at the end of the `<script>` block: `MobilePager` and `MobileSheet`. Both are no-ops above 700px. |
| **DOM changes to existing markup** | Minimal: the three view divs become a `pager-track`, the `.modal-overlay` gets a `data-mobile-sheet` attribute. |
| **Theming** | Reuses your existing `--accent`, `--bg`, `--ink-*` tokens. Adds 2 mobile-only tokens (`--glass`, `--glass-border`). |
| **Hide existing chrome on mobile** | Hide `.topbar` (replace with floating glass), hide `.mobile-nav` (replace with floating glass tab bar), hide `.era-scrubber`. |

---

## 1 · CSS to add

Append to the existing `<style>` block, right **before the closing `</style>`** tag.

```css
/* ─────────────────────────────────────────────────────────────
   MOBILE MODERNIZATION — Hearth & Parchment
   Cinematic + fluid mobile experience. Desktop untouched.
   ───────────────────────────────────────────────────────────── */

@media (max-width: 700px) {
  /* Glass tokens — derived from your existing palette */
  :root, [data-theme="dark"] {
    --glass:        rgba(12, 10, 8, 0.62);
    --glass-border: rgba(255, 255, 255, 0.10);
    --hero-veil:    linear-gradient(180deg, transparent 0%, rgba(8,6,4,0.85) 100%);
  }
  [data-theme="light"] {
    --glass:        rgba(253, 248, 240, 0.72);
    --glass-border: rgba(70, 45, 25, 0.10);
    --hero-veil:    linear-gradient(180deg, transparent 0%, rgba(42,31,24,0.85) 100%);
  }

  /* Hide desktop chrome */
  .topbar { display: none; }
  .mobile-nav { display: none !important; }   /* replaced by .m-tabbar */
  .era-scrubber { display: none !important; }
  .scroll-top-btn { display: none !important; }
  .stats-banner { display: none; }            /* eras shown as horizontal rail in m-home-header */

  /* Body becomes a vertical pager container */
  body { overflow: hidden; height: 100dvh; }
  .main-area { padding-top: 0; }
  .content-area { padding: 0 !important; height: 100dvh; }

  /* ── 1 · Floating glass top bar (search) ─────────────────── */
  .m-topbar {
    position: fixed; top: 12px; left: 12px; right: 12px; height: 52px;
    border-radius: 26px;
    background: var(--glass);
    -webkit-backdrop-filter: blur(20px) saturate(140%);
            backdrop-filter: blur(20px) saturate(140%);
    border: 1px solid var(--glass-border);
    display: flex; align-items: center; gap: 12px;
    padding: 0 18px; z-index: 60;
    transition: transform 0.28s cubic-bezier(0.2,0.7,0.3,1), opacity 0.2s;
  }
  .m-topbar.hidden { transform: translateY(-130%); opacity: 0; pointer-events: none; }
  .m-topbar-icon { font-size: 16px; color: var(--ink-3); }
  .m-topbar-input {
    flex: 1; background: none; border: none; outline: none;
    font-family: 'EB Garamond', Georgia, serif; font-style: italic;
    font-size: 15px; color: var(--ink); min-width: 0;
  }
  .m-topbar-input::placeholder { color: var(--ink-3); }
  .m-topbar-badge {
    padding: 5px 11px; border-radius: 12px;
    background: var(--accent-glow); border: 1px solid var(--accent);
    font-family: 'JetBrains Mono', monospace; font-size: 9.5px;
    letter-spacing: 0.1em; text-transform: uppercase;
    color: var(--accent); font-weight: 600;
  }

  /* ── 2 · Pager (Grid · Timeline · Map swipeable) ──────────── */
  .m-pager {
    position: fixed; inset: 0;
    display: flex; flex-direction: row;
    width: 300vw;                         /* 3 panes */
    height: 100dvh;
    transform: translate3d(0, 0, 0);
    transition: transform 0.34s cubic-bezier(0.2,0.7,0.3,1);
    will-change: transform;
  }
  .m-pager.dragging { transition: none; }
  .m-pane {
    width: 100vw; height: 100dvh;
    overflow-y: auto; overflow-x: hidden;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    padding: 76px 16px 130px;             /* clear glass topbar + tabbar */
    overscroll-behavior-y: contain;
  }
  .m-pane::-webkit-scrollbar { display: none; }
  .m-pane[data-pane="map"] { padding: 0; }   /* map is full-bleed */

  /* ── 3 · Floating glass tab bar (auto-hide on scroll-down) ── */
  .m-tabbar {
    position: fixed; left: 16px; right: 16px; bottom: 24px; height: 64px;
    border-radius: 32px;
    background: var(--glass);
    -webkit-backdrop-filter: blur(24px) saturate(140%);
            backdrop-filter: blur(24px) saturate(140%);
    border: 1px solid var(--glass-border);
    box-shadow: 0 12px 40px rgba(0,0,0,0.35),
                inset 0 1px 0 rgba(255,255,255,0.06);
    display: flex; align-items: center; justify-content: space-around;
    z-index: 60;
    transition: transform 0.32s cubic-bezier(0.2,0.7,0.3,1), opacity 0.2s;
  }
  [data-theme="light"] .m-tabbar {
    box-shadow: 0 12px 40px rgba(80,45,20,0.18),
                inset 0 1px 0 rgba(255,255,255,0.7);
  }
  .m-tabbar.hidden { transform: translateY(140%); opacity: 0; pointer-events: none; }
  .m-tab {
    display: flex; flex-direction: column; align-items: center; gap: 2px;
    padding: 8px 14px; border-radius: 22px;
    background: none; border: none; cursor: pointer;
    transition: background 0.18s;
  }
  .m-tab.active { background: var(--accent-glow); }
  .m-tab-icon { font-size: 20px; line-height: 1; color: var(--ink-3); }
  .m-tab-label {
    font-family: 'JetBrains Mono', monospace; font-size: 9.5px;
    font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase;
    color: var(--ink-3);
  }
  .m-tab.active .m-tab-icon,
  .m-tab.active .m-tab-label { color: var(--accent); }

  /* Pager indicator dots above tabbar */
  .m-pager-dots {
    position: fixed; left: 0; right: 0; bottom: 96px;
    display: flex; justify-content: center; gap: 5px;
    z-index: 59; pointer-events: none;
    transition: opacity 0.2s;
  }
  .m-pager-dot {
    width: 5px; height: 5px; border-radius: 3px;
    background: var(--ink-4); transition: all 0.3s;
  }
  .m-pager-dot.active { width: 22px; background: var(--accent); }

  /* ── 4 · Era hero card (used as pinned timeline header) ───── */
  .m-era-hero {
    position: relative; height: 220px; margin: -76px -16px 0;
    overflow: hidden;
    background: linear-gradient(180deg,
      var(--era-tint, var(--accent)) 0%,
      transparent 100%);
  }
  .m-era-hero::before {
    content: ''; position: absolute; inset: 0;
    background: linear-gradient(180deg, transparent 50%, var(--bg) 100%);
  }
  .m-era-hero-body {
    position: absolute; left: 24px; right: 24px; bottom: 24px;
  }
  .m-era-hero-eyebrow {
    font-family: 'JetBrains Mono', monospace; font-size: 11px;
    letter-spacing: 0.22em; text-transform: uppercase;
    color: var(--accent); margin-bottom: 8px;
  }
  .m-era-hero-title {
    font-family: 'Cormorant Garamond', serif; font-size: 44px;
    font-weight: 400; font-style: italic; color: var(--ink);
    letter-spacing: -0.02em; line-height: 0.95;
  }
  .m-era-hero-meta {
    font-family: 'EB Garamond', Georgia, serif; font-size: 13px;
    color: var(--ink-3); margin-top: 10px; letter-spacing: 0.02em;
  }
  .m-era-scrubber-mini {
    position: absolute; top: 70px; left: 24px;
    display: flex; gap: 6px; align-items: center;
  }
  .m-era-scrubber-mini-dot {
    width: 6px; height: 6px; border-radius: 3px;
    background: var(--ink-4); transition: all 0.3s;
  }
  .m-era-scrubber-mini-dot.active { width: 28px; background: var(--accent); }

  /* ── 5 · Timeline cards (single-column, era-tinted spine) ── */
  .m-timeline-rail { position: relative; padding-left: 56px; padding-top: 20px; }
  .m-timeline-rail::before {
    content: ''; position: absolute; left: 32px; top: 12px; bottom: 12px;
    width: 1px;
    background: linear-gradient(180deg, transparent, var(--border) 8%,
                var(--border) 92%, transparent);
  }
  .m-timeline-entry {
    position: relative; margin-bottom: 24px;
    animation: mhRise 0.6s both;
  }
  .m-timeline-node {
    position: absolute; left: -32px; top: 24px;
    width: 14px; height: 14px; border-radius: 50%;
    background: var(--bg); border: 2px solid var(--era-tint, var(--accent));
    display: flex; align-items: center; justify-content: center;
  }
  .m-timeline-node::after {
    content: ''; width: 6px; height: 6px; border-radius: 3px;
    background: var(--era-tint, var(--accent));
  }
  .m-timeline-year {
    position: absolute; left: -56px; top: -2px; width: 36px;
    text-align: right;
    font-family: 'JetBrains Mono', monospace; font-size: 10px;
    color: var(--ink-3); letter-spacing: 0.06em;
  }
  .m-timeline-card {
    margin-right: 16px; border-radius: 14px; overflow: hidden;
    background: var(--surface); border: 1px solid var(--border-light);
    display: flex; min-height: 100px;
  }
  .m-timeline-card-img {
    width: 96px; flex-shrink: 0; position: relative;
    background: linear-gradient(135deg,
      color-mix(in oklch, var(--era-tint, var(--accent)) 20%, transparent),
      color-mix(in oklch, var(--era-tint, var(--accent)) 5%, transparent));
    display: flex; align-items: center; justify-content: center;
    font-family: 'Cormorant Garamond', serif; font-style: italic;
    font-size: 56px; color: var(--era-tint, var(--accent)); font-weight: 300;
  }
  .m-timeline-card-img img {
    position: absolute; inset: 0;
    width: 100%; height: 100%; object-fit: cover; object-position: center top;
  }
  .m-timeline-card-body { flex: 1; padding: 14px; min-width: 0; }
  .m-timeline-card-eyebrow {
    font-family: 'JetBrains Mono', monospace; font-size: 9px;
    letter-spacing: 0.16em; text-transform: uppercase;
    color: var(--era-tint, var(--accent)); margin-bottom: 4px;
  }
  .m-timeline-card-name {
    font-family: 'Cormorant Garamond', serif; font-size: 19px; font-weight: 500;
    color: var(--ink); line-height: 1.1; letter-spacing: -0.01em; margin-bottom: 4px;
  }
  .m-timeline-card-meta {
    font-family: 'Inter Tight', 'Inter', sans-serif; font-size: 11.5px;
    color: var(--ink-3); margin-bottom: 8px;
  }
  .m-timeline-card-desc {
    font-family: 'EB Garamond', Georgia, serif; font-size: 13.5px;
    color: var(--ink-2); line-height: 1.45;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
    overflow: hidden;
  }

  /* ── 6 · Bottom-sheet figure profile ───────────────────────── */
  .modal-overlay[data-mobile-sheet="open"] {
    background: rgba(0,0,0,0.45);
    align-items: flex-end; padding: 0;
  }
  [data-mobile-sheet="open"] .modal {
    width: 100%; max-width: 100%; min-height: 92dvh;
    border-radius: 28px 28px 0 0;
    animation: m-sheet-rise 0.34s cubic-bezier(0.2,0.7,0.3,1);
    overflow: hidden;
  }
  @keyframes m-sheet-rise {
    from { transform: translateY(100%); }
    to   { transform: translateY(0); }
  }
  [data-mobile-sheet="open"] .modal-close {
    top: 14px; right: 14px;
    background: rgba(0,0,0,0.32);
    -webkit-backdrop-filter: blur(12px);
            backdrop-filter: blur(12px);
    border: 1px solid rgba(255,255,255,0.18);
    color: #fff;
  }
  /* Drag handle */
  [data-mobile-sheet="open"] .modal::before {
    content: ''; position: absolute; top: 8px; left: 50%;
    transform: translateX(-50%);
    width: 40px; height: 4px; border-radius: 2px;
    background: rgba(255,255,255,0.6); z-index: 30;
  }
  /* Profile hero — full-bleed, fades into content */
  [data-mobile-sheet="open"] .mpro-layout { display: block; }
  [data-mobile-sheet="open"] .mpro-left {
    position: relative; width: 100%; height: 56dvh;
    border-bottom: none; border-right: none;
    flex-direction: column !important;
  }
  [data-mobile-sheet="open"] .mpro-img-wrap {
    width: 100% !important; height: 100% !important;
    aspect-ratio: auto; max-height: none;
    border-radius: 0; overflow: hidden;
    animation: m-kenburns 18s ease-in-out infinite alternate;
    transform-origin: center 30%;
  }
  @keyframes m-kenburns {
    0%   { transform: scale(1.05); }
    100% { transform: scale(1.18) translate(-2%,-3%); }
  }
  [data-mobile-sheet="open"] .mpro-img-wrap::after {
    content: ''; position: absolute; inset: 0;
    background: var(--hero-veil);
    pointer-events: none;
  }
  /* Title sits over hero */
  [data-mobile-sheet="open"] .mpro-meta-panel { display: none; }
  [data-mobile-sheet="open"] .mpro-hero-overlay {
    position: absolute; left: 24px; right: 24px; bottom: 28px;
    z-index: 5; pointer-events: none;
  }
  /* Right column = bottom-sheet body */
  [data-mobile-sheet="open"] .mpro-right {
    padding: 22px 22px 130px;
  }
  [data-mobile-sheet="open"] .mpro-name {
    font-size: 56px; line-height: 0.95; color: #fff;
    text-shadow: 0 4px 24px rgba(0,0,0,0.6);
  }
  [data-mobile-sheet="open"] .mpro-stat-row {
    display: grid; grid-template-columns: repeat(3, 1fr);
    gap: 14px; padding-top: 14px; margin-top: 18px;
    border-top: 1px solid var(--border-light);
  }

  /* ── 7 · Map cinematic markers + glass cards ──────────────── */
  .m-map-wrap { position: relative; width: 100vw; height: 100dvh; padding: 0; }
  .m-map-wrap #mapContainer {
    width: 100%; height: 100% !important;
    border-radius: 0 !important; border: none !important;
  }
  .m-map-overlay { position: absolute; inset: 0; pointer-events: none; z-index: 400; }
  .m-map-era-rail {
    position: absolute; top: 80px; left: 0; right: 0;
    display: flex; gap: 8px; padding: 0 16px;
    overflow-x: auto; pointer-events: auto;
    scrollbar-width: none;
  }
  .m-map-era-rail::-webkit-scrollbar { display: none; }
  .m-map-era-chip {
    flex-shrink: 0; padding: 7px 14px; border-radius: 18px;
    background: var(--glass);
    -webkit-backdrop-filter: blur(12px); backdrop-filter: blur(12px);
    border: 1px solid var(--glass-border);
    font-family: 'Inter Tight', 'Inter', sans-serif;
    font-size: 11px; font-weight: 600;
    color: var(--ink-2); white-space: nowrap;
    display: flex; align-items: center; gap: 6px;
  }
  .m-map-era-chip[data-active="true"] {
    background: var(--era-tint, var(--accent)); color: #fff;
    border-color: var(--era-tint, var(--accent));
  }
  .m-map-selected-card {
    position: absolute; left: 16px; right: 16px; bottom: 110px;
    border-radius: 18px; padding: 14px;
    background: var(--glass);
    -webkit-backdrop-filter: blur(24px) saturate(140%);
            backdrop-filter: blur(24px) saturate(140%);
    border: 1px solid var(--glass-border);
    box-shadow: 0 10px 36px rgba(0,0,0,0.3);
    display: flex; gap: 12px; align-items: center;
    pointer-events: auto;
    transform: translateY(140%); opacity: 0;
    transition: transform 0.32s cubic-bezier(0.2,0.7,0.3,1), opacity 0.2s;
  }
  .m-map-selected-card.shown { transform: translateY(0); opacity: 1; }

  /* Pulsing era-tinted Leaflet marker (override default) */
  .m-map-pin {
    width: 12px; height: 12px; border-radius: 50%;
    background: var(--era-tint, var(--accent));
    box-shadow: 0 0 18px var(--era-tint, var(--accent)),
                0 0 8px color-mix(in oklch, var(--era-tint) 70%, transparent);
    border: 2px solid var(--bg);
  }
  .m-map-pin.is-selected {
    width: 18px; height: 18px;
    box-shadow: 0 0 28px var(--era-tint), 0 0 8px var(--era-tint);
    animation: m-pulse 2.5s ease-in-out infinite;
  }
  @keyframes m-pulse {
    0%, 100% { opacity: 0.6; }
    50%      { opacity: 1; }
  }

  /* ── 8 · Home pull-to-reveal search/filters ────────────────── */
  .m-home-header {
    padding: 20px 24px 14px;
    animation: mhRise 0.5s both;
  }
  .m-home-eyebrow {
    font-family: 'JetBrains Mono', monospace; font-size: 10px;
    letter-spacing: 0.22em; text-transform: uppercase;
    color: var(--accent); margin-bottom: 8px;
  }
  .m-home-title {
    font-family: 'Cormorant Garamond', serif; font-size: 38px;
    font-weight: 400; color: var(--ink); line-height: 1.0;
    letter-spacing: -0.025em;
  }
  .m-home-title em { color: var(--accent); }

  /* Quick filter chip strip below glass topbar (revealed state) */
  .m-quick-filters {
    position: fixed; top: 76px; left: 0; right: 0;
    padding: 0 16px 12px; display: flex; gap: 7px;
    overflow-x: auto; z-index: 55;
    scrollbar-width: none;
    transform: translateY(-8px); opacity: 0;
    pointer-events: none; transition: all 0.24s cubic-bezier(0.2,0.7,0.3,1);
  }
  .m-quick-filters.shown {
    transform: translateY(0); opacity: 1; pointer-events: auto;
  }
  .m-quick-filters::-webkit-scrollbar { display: none; }
  .m-quick-filter {
    flex-shrink: 0; padding: 5px 12px; border-radius: 12px;
    background: transparent; border: 1px solid var(--border);
    font-family: 'Inter Tight', 'Inter', sans-serif;
    font-size: 11px; font-weight: 500;
    color: var(--ink-2); white-space: nowrap;
  }
  .m-quick-filter.active {
    background: var(--ink); border-color: var(--ink); color: var(--bg);
  }

  /* ── 9 · Featured photo card on Home ───────────────────────── */
  .m-home-featured {
    position: relative; margin: 8px 16px 18px;
    height: 280px; border-radius: 18px; overflow: hidden;
    border: 1px solid var(--border-light);
  }
  .m-home-featured-img {
    position: absolute; inset: 0;
    background-size: cover; background-position: center;
    animation: m-kenburns 18s ease-in-out infinite alternate;
  }
  .m-home-featured-veil {
    position: absolute; inset: 0; background: var(--hero-veil);
  }
  .m-home-featured-body {
    position: absolute; left: 18px; right: 18px; bottom: 18px; color: #fff;
  }

  /* Eras horizontal rail on Home */
  .m-home-eras {
    display: flex; gap: 12px; padding: 0 18px 4px;
    overflow-x: auto; scrollbar-width: none;
  }
  .m-home-eras::-webkit-scrollbar { display: none; }
  .m-home-era-card {
    width: 140px; flex-shrink: 0; padding: 14px;
    border-radius: 14px;
    background: var(--surface); border: 1px solid var(--border-light);
  }
  .m-home-era-card-bar {
    height: 3px; width: 28px; border-radius: 2px;
    background: var(--era-tint, var(--accent)); margin-bottom: 12px;
  }
  .m-home-era-card-count {
    font-family: 'Cormorant Garamond', serif; font-size: 30px;
    font-weight: 400; color: var(--ink); line-height: 1;
    letter-spacing: -0.02em;
  }
  .m-home-era-card-name {
    font-family: 'Cormorant Garamond', serif; font-style: italic;
    font-size: 14px; color: var(--ink); margin-top: 4px;
  }
  .m-home-era-card-dates {
    font-family: 'JetBrains Mono', monospace; font-size: 9px;
    color: var(--ink-3); margin-top: 6px; letter-spacing: 0.04em;
  }

  /* ── 10 · Shared keyframes ─────────────────────────────────── */
  @keyframes mhRise {
    from { opacity: 0; transform: translateY(14px); }
    to   { opacity: 1; transform: translateY(0); }
  }
}

/* iPad / large-phone tweaks */
@media (max-width: 700px) and (min-width: 480px) {
  .m-tabbar { left: 24px; right: 24px; }
}
```

---

## 2 · DOM changes to `frontend/index.html`

### 2a · Wrap the three view divs in a pager (mobile only)

The pager wrapping is **purely CSS-driven on mobile** — desktop ignores it. Inside `.content-area`, change:

```html
<!-- Grid View -->
<div id="view-grid">…</div>
<!-- Timeline View -->
<div id="view-timeline" style="display:none">…</div>
<!-- Map View -->
<div id="view-map" style="display:none">…</div>
```

…to:

```html
<div class="m-pager" id="mPager">
  <div class="m-pane" data-pane="grid"   id="view-grid">…</div>
  <div class="m-pane" data-pane="timeline" id="view-timeline" style="display:none">…</div>
  <div class="m-pane" data-pane="map"    id="view-map"    style="display:none">…</div>
</div>
```

> **Important:** keep `display:none` on Timeline / Map for desktop-init compatibility. The mobile-pager JS strips the inline `display` once it activates. Desktop `switchView()` still works untouched.

### 2b · Add the new mobile chrome above the `<div class="main-area">`

Insert just **after** `<div class="sidebar-overlay" …></div>`, **before** `<div class="main-area">`:

```html
<!-- ── MOBILE CHROME (mobile-only; CSS-hidden on desktop) ───── -->
<div class="m-topbar" id="mTopbar">
  <span class="m-topbar-icon">⌕</span>
  <input class="m-topbar-input" id="mSearchInput"
         placeholder="Search figures, places, beliefs…" autocomplete="off" />
  <span class="m-topbar-badge" id="mResultBadge">260</span>
</div>

<div class="m-quick-filters" id="mQuickFilters">
  <!-- Populated by JS — same chips as desktop era-chips -->
</div>

<nav class="m-tabbar" id="mTabbar" role="tablist">
  <button class="m-tab active" data-pane="grid"     onclick="mPagerGoTo(0)">
    <span class="m-tab-icon">⊞</span>
    <span class="m-tab-label">Browse</span>
  </button>
  <button class="m-tab"        data-pane="timeline" onclick="mPagerGoTo(1)">
    <span class="m-tab-icon">⌇</span>
    <span class="m-tab-label">Timeline</span>
  </button>
  <button class="m-tab"        data-pane="map"      onclick="mPagerGoTo(2)">
    <span class="m-tab-icon">◔</span>
    <span class="m-tab-label">Map</span>
  </button>
  <button class="m-tab"        data-pane="saved"    onclick="openSaved()">
    <span class="m-tab-icon" style="font-family:'Cormorant Garamond',serif;font-style:italic">✦</span>
    <span class="m-tab-label">Saved</span>
  </button>
</nav>

<div class="m-pager-dots" id="mPagerDots" aria-hidden="true">
  <div class="m-pager-dot active"></div>
  <div class="m-pager-dot"></div>
  <div class="m-pager-dot"></div>
</div>
```

### 2c · Add a `data-mobile-sheet` hook to the modal overlay

Change:

```html
<div class="modal-overlay" id="modal" style="display:none" onclick="closeOnOverlay(event)">
```

to:

```html
<div class="modal-overlay" id="modal" style="display:none" data-mobile-sheet="closed" onclick="closeOnOverlay(event)">
```

This is set to `"open"` whenever `openFigure()` runs on mobile (see JS section).

---

## 3 · JS to add

Two small modules. Add **at the end of the existing `<script>` block**, just before `</script>`.

### 3a · `MobilePager` — swipe + dot indicator + tab sync

```javascript
// ─────────────────────────────────────────────────────────────
// MOBILE PAGER — horizontal swipe between Grid / Timeline / Map
// No-op on desktop. Wraps the existing switchView() so existing
// links and buttons keep working.
// ─────────────────────────────────────────────────────────────
(function () {
  const MQ = window.matchMedia('(max-width: 700px)');
  const PANES = ['grid', 'timeline', 'map'];
  let idx = 0;
  let dragStartX = null, dragStartY = null, dragDx = 0, locked = null;
  let lastScrollY = 0, hideTimer = null;

  const pager = () => document.getElementById('mPager');
  const dots  = () => document.querySelectorAll('#mPagerDots .m-pager-dot');
  const tabs  = () => document.querySelectorAll('.m-tab[data-pane]');

  function setIdx(i, animate = true) {
    idx = Math.max(0, Math.min(2, i));
    const p = pager();
    if (!p) return;
    p.classList.toggle('dragging', !animate);
    p.style.transform = `translate3d(-${idx * 100}vw, 0, 0)`;
    dots().forEach((d, k) => d.classList.toggle('active', k === idx));
    tabs().forEach(t => t.classList.toggle('active', t.dataset.pane === PANES[idx]));

    // Make the active pane visible (override desktop's display:none)
    PANES.forEach((name, k) => {
      const el = document.getElementById('view-' + name);
      if (el) el.style.display = '';   // CSS .m-pane handles layout
    });

    // Hand off to existing logic so it loads timeline/map data on first show
    if (typeof switchView === 'function') switchView(PANES[idx]);

    // Haptic
    if (navigator.vibrate) navigator.vibrate(8);
  }
  window.mPagerGoTo = setIdx;

  function activate() {
    const p = pager();
    if (!p) return;
    p.style.display = 'flex';   // CSS handles the rest

    // Swipe handlers
    p.addEventListener('touchstart', onStart, { passive: true });
    p.addEventListener('touchmove',  onMove,  { passive: false });
    p.addEventListener('touchend',   onEnd);

    // Auto-hide tabbar/topbar on scroll-down per pane
    document.querySelectorAll('.m-pane').forEach(pane => {
      pane.addEventListener('scroll', onPaneScroll, { passive: true });
    });

    setIdx(0, false);
  }
  function deactivate() {
    const p = pager();
    if (p) p.style.transform = '';
  }

  function onStart(e) {
    if (e.touches.length !== 1) return;
    dragStartX = e.touches[0].clientX;
    dragStartY = e.touches[0].clientY;
    dragDx = 0; locked = null;
  }
  function onMove(e) {
    if (dragStartX == null) return;
    const dx = e.touches[0].clientX - dragStartX;
    const dy = e.touches[0].clientY - dragStartY;
    if (locked == null) {
      if (Math.abs(dx) > 8 || Math.abs(dy) > 8) {
        locked = Math.abs(dx) > Math.abs(dy) ? 'x' : 'y';
      } else return;
    }
    if (locked !== 'x') return;
    e.preventDefault();
    dragDx = dx;
    const p = pager();
    p.classList.add('dragging');
    p.style.transform = `translate3d(calc(-${idx * 100}vw + ${dx}px), 0, 0)`;
  }
  function onEnd() {
    if (locked === 'x') {
      const threshold = window.innerWidth * 0.18;
      if (dragDx < -threshold && idx < 2) setIdx(idx + 1);
      else if (dragDx > threshold && idx > 0) setIdx(idx - 1);
      else setIdx(idx);
    }
    dragStartX = dragStartY = null; dragDx = 0; locked = null;
  }

  // Hide chrome on scroll-down, reveal on scroll-up
  function onPaneScroll(e) {
    const y = e.target.scrollTop;
    const dy = y - lastScrollY;
    const tb = document.getElementById('mTabbar');
    const top = document.getElementById('mTopbar');
    const dots = document.getElementById('mPagerDots');
    if (Math.abs(dy) > 6) {
      const hide = dy > 0 && y > 80;
      tb && tb.classList.toggle('hidden', hide);
      top && top.classList.toggle('hidden', hide);
      if (dots) dots.style.opacity = hide ? '0' : '1';
    }
    lastScrollY = y;
  }

  // Desktop ↔ mobile switching
  function check() { MQ.matches ? activate() : deactivate(); }
  MQ.addEventListener('change', check);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', check);
  } else {
    check();
  }

  // Also: when desktop switchView() is called, sync our pager
  const _origSwitch = window.switchView;
  if (_origSwitch) {
    window.switchView = function (view) {
      _origSwitch.apply(this, arguments);
      const i = PANES.indexOf(view);
      if (i >= 0 && MQ.matches) setIdx(i);
    };
  }
})();
```

### 3b · `MobileSheet` — bottom-sheet wrapper around the existing modal

```javascript
// ─────────────────────────────────────────────────────────────
// MOBILE SHEET — wraps the existing #modal as a draggable bottom
// sheet on small screens. Desktop modal behavior is unchanged.
// Listens for the modal opening (MutationObserver) and patches
// the DOM in-place so openFigure() needs no changes.
// ─────────────────────────────────────────────────────────────
(function () {
  const MQ = window.matchMedia('(max-width: 700px)');
  const modal = () => document.getElementById('modal');

  function patchOpen() {
    const m = modal();
    if (!m || !MQ.matches) return;
    m.dataset.mobileSheet = 'open';

    // Hoist the figure name onto the hero as an absolutely-positioned
    // overlay (only if not already done by openFigure). Look for an
    // existing .mpro-name and clone its text into a hero overlay.
    const left = m.querySelector('.mpro-left');
    const name = m.querySelector('.mpro-name');
    if (left && name && !m.querySelector('.mpro-hero-overlay')) {
      const overlay = document.createElement('div');
      overlay.className = 'mpro-hero-overlay';
      overlay.appendChild(name.cloneNode(true));
      // optional: clone the role/dates eyebrow
      const eyebrow = m.querySelector('.mpro-badge');
      if (eyebrow) overlay.insertBefore(eyebrow.cloneNode(true), overlay.firstChild);
      left.appendChild(overlay);
    }

    // Add drag-to-dismiss
    bindDrag();
    if (navigator.vibrate) navigator.vibrate(10);
  }

  function patchClose() {
    const m = modal();
    if (!m) return;
    m.dataset.mobileSheet = 'closed';
    const overlay = m.querySelector('.mpro-hero-overlay');
    overlay && overlay.remove();
  }

  let dragStartY = null, dragDy = 0, sheetEl = null;
  function bindDrag() {
    const m = modal();
    sheetEl = m.querySelector('.modal');
    if (!sheetEl) return;
    sheetEl.addEventListener('touchstart', onStart, { passive: true });
    sheetEl.addEventListener('touchmove',  onMove,  { passive: false });
    sheetEl.addEventListener('touchend',   onEnd);
  }
  function onStart(e) {
    // Only start drag from the top 60px (handle area) to not steal scroll
    if (sheetEl.scrollTop > 0) return;
    if (e.touches[0].clientY > sheetEl.getBoundingClientRect().top + 60) return;
    dragStartY = e.touches[0].clientY;
  }
  function onMove(e) {
    if (dragStartY == null) return;
    const dy = e.touches[0].clientY - dragStartY;
    if (dy < 0) return;
    e.preventDefault();
    dragDy = dy;
    sheetEl.style.transform = `translateY(${dy}px)`;
    sheetEl.style.transition = 'none';
  }
  function onEnd() {
    if (dragStartY == null) return;
    sheetEl.style.transition = '';
    if (dragDy > 120) {
      sheetEl.style.transform = 'translateY(100%)';
      setTimeout(() => {
        if (typeof closeModal === 'function') closeModal();
        sheetEl.style.transform = '';
      }, 280);
    } else {
      sheetEl.style.transform = '';
    }
    dragStartY = null; dragDy = 0;
  }

  // Watch the modal display style — the existing openFigure()
  // sets style.display = 'flex'. We trigger our patches off that.
  const m = modal();
  if (m) {
    let lastDisplay = m.style.display;
    new MutationObserver(() => {
      const d = m.style.display;
      if (d !== lastDisplay) {
        if (d === 'flex' || d === 'block') patchOpen();
        else patchClose();
        lastDisplay = d;
      }
    }).observe(m, { attributes: true, attributeFilter: ['style'] });
  }
})();
```

### 3c · Mobile search → existing search input proxy

The floating glass topbar needs to drive the same logic as the desktop search. Add this **right before** the IIFE blocks above:

```javascript
// Mobile glass-topbar search input proxies to the desktop input
(function () {
  const mSearch = document.getElementById('mSearchInput');
  const dSearch = document.getElementById('searchInput');
  if (!mSearch || !dSearch) return;

  mSearch.addEventListener('input', () => {
    dSearch.value = mSearch.value;
    dSearch.dispatchEvent(new Event('input', { bubbles: true }));
  });

  // Reflect record count to the mobile badge
  const badge = document.getElementById('mResultBadge');
  if (badge) {
    new MutationObserver(() => {
      const n = (document.getElementById('headerCount')?.textContent || '').match(/\d+/);
      if (n) badge.textContent = n[0];
    }).observe(document.getElementById('headerCount'), {
      childList: true, characterData: true, subtree: true,
    });
  }
})();
```

### 3d · Pull-down to reveal quick filters

Append to the same script block:

```javascript
// Pull-down on the Grid pane reveals quick-filter chips
(function () {
  const filters = document.getElementById('mQuickFilters');
  if (!filters) return;
  // Populate from existing era chips
  document.querySelectorAll('.era-chips .era-chip').forEach(chip => {
    const clone = chip.cloneNode(true);
    clone.className = 'm-quick-filter' + (chip.classList.contains('active') ? ' active' : '');
    clone.removeAttribute('onclick');
    clone.addEventListener('click', () => chip.click());
    filters.appendChild(clone);
  });

  let startY = null, dy = 0, pulling = false;
  const grid = document.getElementById('view-grid');
  if (!grid) return;
  grid.addEventListener('touchstart', e => {
    if (grid.scrollTop > 0) return;
    startY = e.touches[0].clientY;
  }, { passive: true });
  grid.addEventListener('touchmove', e => {
    if (startY == null) return;
    dy = e.touches[0].clientY - startY;
    if (dy > 60 && !pulling) {
      pulling = true;
      filters.classList.add('shown');
      if (navigator.vibrate) navigator.vibrate(8);
    }
  }, { passive: true });
  grid.addEventListener('touchend', () => {
    startY = null; dy = 0;
    setTimeout(() => { pulling = false; }, 400);
  });

  // Tap outside to dismiss
  document.addEventListener('click', e => {
    if (!filters.classList.contains('shown')) return;
    if (!filters.contains(e.target) && !e.target.closest('.m-topbar')) {
      filters.classList.remove('shown');
    }
  });
})();
```

---

## 4 · Per-component refinements

### 4a · Era hero pinning on Timeline (`loadChronoView`)

Inside `loadChronoView()`, when rendering each era intro card on mobile, wrap it in `.m-era-hero`. The existing scroll-pinning observer (`chronoScrollListener`) already updates the active era; we just need to set `--era-tint` on the body when it changes. In the existing scroll handler, replace the active-era update with:

```javascript
function setActiveChronoEra(idx) {
  const tints = ['var(--era1)', 'var(--era2)', 'var(--era3)', 'var(--era4)'];
  document.documentElement.style.setProperty('--era-tint', tints[idx]);
  document.querySelectorAll('.m-era-scrubber-mini-dot')
    .forEach((d, k) => d.classList.toggle('active', k === idx));
}
```

### 4b · Timeline cards — switch to mobile markup at small widths

In `renderChronoCard()` (or wherever you build each timeline entry), branch:

```javascript
const isMobile = window.matchMedia('(max-width: 700px)').matches;
return isMobile ? renderMobileTimelineCard(f) : renderDesktopChronoCard(f);
```

`renderMobileTimelineCard(f)` should produce the markup pattern from §1 (`.m-timeline-entry > .m-timeline-node + .m-timeline-year + .m-timeline-card`) and set `style="--era-tint: ${ERA_HEX[f.era_idx]}"` on the `.m-timeline-entry`.

### 4c · Map — glass overlays + cinematic markers

The existing Leaflet map stays — no replacement needed. Add the overlay layer **inside `<div id="view-map">`**, after `#mapContainer`:

```html
<div class="m-map-overlay">
  <div class="m-map-era-rail" id="mMapEraRail">
    <!-- chips populated by JS — same eras as the home rail -->
  </div>
  <div class="m-map-selected-card" id="mMapSelected">
    <!-- populated when a marker is clicked -->
  </div>
</div>
```

In the marker-creation loop, replace the default Leaflet pin with a div icon:

```javascript
const era = f.era_idx ?? 0;
const tints = ['#6db3d4','#9b7fd4','#d4903a','#c96a3a'];
const icon = L.divIcon({
  className: '',
  iconSize: [16, 16], iconAnchor: [8, 8],
  html: `<div class="m-map-pin" style="--era-tint:${tints[era]}"></div>`,
});
const marker = L.marker([f.lat, f.lng], { icon }).addTo(mapInstance);
marker.on('click', () => {
  document.querySelectorAll('.m-map-pin').forEach(p => p.classList.remove('is-selected'));
  marker.getElement()?.querySelector('.m-map-pin')?.classList.add('is-selected');
  showMobileMapCard(f);
});
```

Add the helper:

```javascript
function showMobileMapCard(f) {
  const card = document.getElementById('mMapSelected');
  if (!card) return;
  const tint = ['#6db3d4','#9b7fd4','#d4903a','#c96a3a'][f.era_idx ?? 0];
  card.innerHTML = `
    <div style="width:56px;height:56px;border-radius:12px;
                background:linear-gradient(135deg,${tint}50,${tint}20);
                display:flex;align-items:center;justify-content:center;
                font-family:'Cormorant Garamond',serif;font-style:italic;
                font-size:32px;color:${tint};flex-shrink:0">${(f.name||'?')[0]}</div>
    <div style="flex:1;min-width:0">
      <div style="font-family:'JetBrains Mono',monospace;font-size:9px;
                  letter-spacing:.16em;text-transform:uppercase;color:${tint};
                  margin-bottom:2px">${f.primary_region || ''} · ${f.born}–${f.death}</div>
      <div style="font-family:'Cormorant Garamond',serif;font-size:18px;
                  font-weight:500;color:var(--ink);line-height:1.1">${f.name}</div>
      <div style="font-family:'Inter Tight',sans-serif;font-size:11.5px;
                  color:var(--ink-3);margin-top:2px">${f.role_office || ''}</div>
    </div>
    <button onclick="openFigure(${f.id})"
            style="width:36px;height:36px;border-radius:18px;background:var(--accent);
                   color:#fff;border:none;font-size:14px;flex-shrink:0">›</button>
  `;
  card.classList.add('shown');
}
```

### 4d · Home grid — replace the era bands with the new hero block

On mobile only, render the home view differently. At the top of `renderGrid()` (or wherever the grid headers are emitted), inject the mobile hero markup if `MQ.matches`:

```javascript
if (window.matchMedia('(max-width: 700px)').matches && state.page === 1) {
  document.getElementById('view-grid').insertAdjacentHTML('afterbegin', `
    <header class="m-home-header">
      <div class="m-home-eyebrow">Today · ${new Date().toLocaleDateString('en-US',{month:'long',day:'numeric'})}</div>
      <h1 class="m-home-title">Two thousand years of <em>faithful witness</em>.</h1>
    </header>
    <div class="m-home-featured" onclick="openFigure(${featuredFigure.id})">
      <div class="m-home-featured-img" style="background-image:url('${featuredFigure.imageUrl||''}')"></div>
      <div class="m-home-featured-veil"></div>
      <div class="m-home-featured-body">
        <div class="m-home-eyebrow" style="color:#f5d8b8">Figure of the week</div>
        <div style="font-family:'Cormorant Garamond',serif;font-size:30px;
                    font-weight:400;line-height:1;letter-spacing:-0.02em">
          ${featuredFigure.name.split(' ')[0]}
          <em style="font-style:italic">${featuredFigure.name.split(' ').slice(1).join(' ')}</em>
        </div>
        <div style="font-family:'Cormorant Garamond',serif;font-style:italic;
                    font-size:13px;margin-top:4px;opacity:0.85">
          ${featuredFigure.role_office} · ${featuredFigure.born}–${featuredFigure.death}
        </div>
      </div>
    </div>
    <div class="m-home-eras">
      ${CHRONO_ERAS.map((e, i) => `
        <div class="m-home-era-card" style="--era-tint:${e.hex}">
          <div class="m-home-era-card-bar"></div>
          <div class="m-home-era-card-count">${eraCounts[i] || '—'}</div>
          <div class="m-home-era-card-name">${e.name}</div>
          <div class="m-home-era-card-dates">${e.dates}</div>
        </div>
      `).join('')}
    </div>
  `);
}
```

(`featuredFigure` = whatever you're currently using for "Figure of the Week"; `eraCounts` = the `.era-band-count` numbers.)

---

## 5 · Suggested commit sequence

For Claude Code, structure the work as 6 small commits so each is reviewable:

1. **`feat(mobile): add glass tokens + mobile chrome HTML`** — §2b + §1 (only the `:root` and `.m-topbar`/`.m-tabbar` blocks).
2. **`feat(mobile): wire pager swipe between Grid/Timeline/Map`** — §2a + §3a.
3. **`feat(mobile): bottom-sheet figure profile`** — §2c + §3b + §1 sheet block.
4. **`feat(mobile): redesign timeline as cinematic rail`** — §1 timeline blocks + §4a + §4b.
5. **`feat(mobile): cinematic map markers + glass selection card`** — §1 map blocks + §4c.
6. **`feat(mobile): home hero + pull-to-reveal quick filters`** — §1 home blocks + §3c + §3d + §4d.

Each can ship independently — each one **leaves the desktop view untouched**.

---

## 6 · Things to verify after applying

| Check | Expected |
|---|---|
| Resize viewport from 1200 → 375 px | Desktop chrome hides, mobile chrome appears at 700 px breakpoint. |
| Tap a card on the Grid pane | Profile opens as bottom sheet (drag handle visible, hero at top). |
| Drag the sheet down >120 px | Closes. |
| Swipe left on Grid | Pager moves to Timeline; tabbar + dots update. |
| Scroll a pane down >80 px | Tabbar + topbar slide off-screen; reappear on scroll-up. |
| Pull down at top of Grid | Quick-filter chip strip slides in below the topbar. |
| Toggle theme | Glass surfaces re-tint correctly; map markers keep era colors. |
| Open figure from Map pin | Bottom sheet uses the `mpro-hero-overlay` (title over hero image). |
| Reduced motion preference | Ken-Burns and rise animations should respect `prefers-reduced-motion` — add `@media (prefers-reduced-motion: reduce) { .mh-grain, .m-home-featured-img, [data-mobile-sheet="open"] .mpro-img-wrap { animation: none; } }`. |

---

## 7 · Files to share with Claude Code

- This document — `Mobile Modernization - Implementation Handoff.md`
- The visual reference — `Church History App - Mobile Modernization.html` (open the artboards while implementing)
- The component sketches — `mobile-hearth.jsx` (for exact spacing / type sizes / colors when in doubt)

That's all. Everything else lives in `frontend/index.html`.
