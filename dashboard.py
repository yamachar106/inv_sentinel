"""
投資ダッシュボード Web UI

ウォッチリスト・ポジション・シグナル・パフォーマンスを
ブラウザで一覧表示する。外部依存なし（Python標準ライブラリのみ）。

Usage:
    python dashboard.py              # http://localhost:8501 で起動
    python dashboard.py --port 9000  # ポート指定
"""

import argparse
import json
import re
import sys
from datetime import date, datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# ─── データ読み込み関数 ───


def _load_json(path: Path) -> dict | list:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def get_portfolio():
    data = _load_json(DATA_DIR / "portfolio.json")
    return data.get("positions", {})


def get_trade_history():
    data = _load_json(DATA_DIR / "portfolio_history.json")
    return data.get("trades", [])


def get_performance_stats():
    trades = get_trade_history()
    if not trades:
        return {
            "total_trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0, "avg_return": 0, "total_profit": 0,
            "profit_factor": 0, "avg_hold_days": 0,
            "avg_win": 0, "avg_loss": 0,
            "max_win": 0, "max_loss": 0,
            "consecutive_wins": 0, "consecutive_losses": 0,
        }
    wins = [t for t in trades if t.get("return_pct", 0) > 0]
    losses = [t for t in trades if t.get("return_pct", 0) <= 0]
    total_win = sum(t.get("profit", 0) for t in wins)
    total_loss = sum(t.get("profit", 0) for t in losses)
    pf = total_win / abs(total_loss) if total_loss != 0 else 0
    returns = [t.get("return_pct", 0) for t in trades]
    hold_days = [t.get("hold_days", 0) for t in trades]
    win_returns = [t.get("return_pct", 0) for t in wins]
    loss_returns = [t.get("return_pct", 0) for t in losses]

    # Consecutive wins/losses
    max_cw = max_cl = cw = cl = 0
    for t in trades:
        if t.get("return_pct", 0) > 0:
            cw += 1
            cl = 0
        else:
            cl += 1
            cw = 0
        max_cw = max(max_cw, cw)
        max_cl = max(max_cl, cl)

    return {
        "total_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(trades) if trades else 0,
        "avg_return": sum(returns) / len(returns) if returns else 0,
        "total_profit": sum(t.get("profit", 0) for t in trades),
        "profit_factor": round(pf, 2),
        "avg_hold_days": sum(hold_days) / len(hold_days) if hold_days else 0,
        "avg_win": sum(win_returns) / len(win_returns) if win_returns else 0,
        "avg_loss": sum(loss_returns) / len(loss_returns) if loss_returns else 0,
        "max_win": max(win_returns) if win_returns else 0,
        "max_loss": min(loss_returns) if loss_returns else 0,
        "consecutive_wins": max_cw,
        "consecutive_losses": max_cl,
    }


def get_signals_recent(days: int = 7):
    signals_dir = DATA_DIR / "signals"
    if not signals_dir.exists():
        return []
    files = sorted(signals_dir.glob("*.json"), reverse=True)[:days]
    result = []
    for f in files:
        data = _load_json(f)
        result.append(data)
    return result


def get_market_regime():
    try:
        from screener.market_regime import detect_regime
        regime = detect_regime()
        return {
            "trend": regime.trend,
            "price": regime.price,
            "sma50": regime.sma50,
            "sma200": regime.sma200,
            "description": regime.description,
        }
    except Exception:
        return {"trend": "UNKNOWN", "price": 0, "sma50": 0, "sma200": 0, "description": "取得失敗"}


def parse_watchlist_md():
    wl_dir = DATA_DIR / "watchlist"
    if not wl_dir.exists():
        return {"title": "", "stocks": [], "details": []}
    files = sorted(wl_dir.glob("*.md"), reverse=True)
    if not files:
        return {"title": "", "stocks": [], "details": []}

    text = files[0].read_text(encoding="utf-8")
    lines = text.split("\n")

    title = lines[0].replace("# ", "") if lines else ""

    # テーブル行をパース
    stocks = []
    in_table = False
    headers = []
    for line in lines:
        if line.startswith("| 推奨"):
            in_table = True
            headers = [h.strip() for h in line.split("|")[1:-1]]
            continue
        if in_table and line.startswith("|---"):
            continue
        if in_table and line.startswith("|"):
            cols = [c.strip() for c in line.split("|")[1:-1]]
            if len(cols) >= len(headers):
                row = {}
                for h, c in zip(headers, cols):
                    row[h] = c.replace("**", "")
                stocks.append(row)
        elif in_table:
            in_table = False

    # 銘柄詳細セクション
    details = []
    current_detail = None
    for line in lines:
        if line.startswith("### "):
            if current_detail:
                details.append(current_detail)
            current_detail = {"header": line.replace("### ", ""), "lines": []}
        elif current_detail and line.startswith("- "):
            current_detail["lines"].append(line[2:])
    if current_detail:
        details.append(current_detail)

    return {"title": title, "stocks": stocks, "details": details}


# ─── API ハンドラ ───


def handle_api(path: str) -> tuple[int, dict | list]:
    if path == "/api/regime":
        return 200, get_market_regime()
    elif path == "/api/portfolio":
        return 200, get_portfolio()
    elif path == "/api/history":
        return 200, get_trade_history()
    elif path == "/api/stats":
        return 200, get_performance_stats()
    elif path == "/api/signals":
        return 200, get_signals_recent()
    elif path == "/api/watchlist":
        return 200, parse_watchlist_md()
    else:
        return 404, {"error": "not found"}


# ─── Favicon: 赤字→黒字のV字転換 ───

FAVICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect width="64" height="64" rx="14" fill="#0d1117"/>
  <!-- 赤字ゾーン（左上→底）: 下落する赤線 -->
  <path d="M10 18 L30 46" stroke="#f85149" stroke-width="5" stroke-linecap="round" fill="none"/>
  <!-- 黒字転換（底→右上）: 上昇する緑線 -->
  <path d="M30 46 L54 14" stroke="#3fb950" stroke-width="5" stroke-linecap="round" fill="none"/>
  <!-- 転換点の丸 -->
  <circle cx="30" cy="46" r="4" fill="#d29922"/>
  <!-- 2x 目標マーク -->
  <text x="48" y="16" font-family="system-ui,sans-serif" font-size="11" font-weight="800" fill="#58a6ff">2x</text>
</svg>"""


# ─── HTML テンプレート ───

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>黒字転換投資システム | inv_kuroten</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<style>
:root {
  --bg: #0d1117; --surface: #161b22; --surface2: #1c2128; --border: #30363d;
  --text: #e6edf3; --text-muted: #8b949e; --text-dim: #6e7681;
  --accent: #58a6ff; --green: #3fb950; --red: #f85149;
  --yellow: #d29922; --purple: #bc8cff; --orange: #f0883e;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: -apple-system, 'Segoe UI', 'Hiragino Kaku Gothic ProN', 'Meiryo', sans-serif; background:var(--bg); color:var(--text); line-height:1.6; font-size:14px; }
a { color:var(--accent); text-decoration:none; }
a:hover { text-decoration:underline; }

.container { max-width:1440px; margin:0 auto; padding:16px 20px; }
header { display:flex; align-items:center; justify-content:space-between; padding:12px 0; border-bottom:1px solid var(--border); margin-bottom:20px; }
header h1 { font-size:18px; font-weight:600; display:flex; align-items:center; gap:8px; }
header h1 .subtitle { font-size:12px; color:var(--text-muted); font-weight:400; }
header .header-right { display:flex; align-items:center; gap:12px; }
header .date { color:var(--text-muted); font-size:13px; }
header .freshness { font-size:11px; color:var(--text-dim); }

/* Tabs */
.tabs { display:flex; gap:2px; margin-bottom:20px; border-bottom:1px solid var(--border); }
.tab { padding:8px 16px; cursor:pointer; color:var(--text-muted); font-size:13px; font-weight:500; border-bottom:2px solid transparent; transition:all .15s; user-select:none; }
.tab:hover { color:var(--text); }
.tab.active { color:var(--accent); border-bottom-color:var(--accent); }
.tab-content { display:none; }
.tab-content.active { display:block; }

/* Cards */
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:24px; }
.card { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:14px 16px; }
.card .label { font-size:11px; color:var(--text-muted); letter-spacing:.5px; margin-bottom:4px; }
.card .value { font-size:26px; font-weight:700; line-height:1.2; }
.card .sub { font-size:11px; color:var(--text-muted); margin-top:4px; line-height:1.4; }

/* Regime */
.regime { display:inline-flex; align-items:center; gap:6px; padding:5px 12px; border-radius:16px; font-weight:600; font-size:13px; }
.regime.BULL { background:rgba(63,185,80,.12); color:var(--green); border:1px solid rgba(63,185,80,.25); }
.regime.BEAR { background:rgba(248,81,73,.12); color:var(--red); border:1px solid rgba(248,81,73,.25); }
.regime.NEUTRAL { background:rgba(210,153,34,.12); color:var(--yellow); border:1px solid rgba(210,153,34,.25); }
.regime.UNKNOWN { background:rgba(139,148,158,.12); color:var(--text-muted); border:1px solid rgba(139,148,158,.25); }

/* Table */
table { width:100%; border-collapse:collapse; font-size:13px; }
th { text-align:left; padding:8px 10px; color:var(--text-muted); font-weight:500; font-size:11px; letter-spacing:.3px; border-bottom:2px solid var(--border); position:sticky; top:0; background:var(--bg); z-index:1; white-space:nowrap; }
td { padding:7px 10px; border-bottom:1px solid var(--border); }
tr:hover td { background:rgba(88,166,255,.04); }
.right { text-align:right; }
.center { text-align:center; }
.mono { font-family:'SF Mono','Cascadia Code','Consolas',monospace; font-size:12px; }

/* Grade badges */
.grade { display:inline-block; padding:2px 8px; border-radius:4px; font-weight:700; font-size:11px; letter-spacing:.3px; }
.grade-S { background:rgba(63,185,80,.15); color:var(--green); border:1px solid rgba(63,185,80,.3); }
.grade-A { background:rgba(88,166,255,.15); color:var(--accent); border:1px solid rgba(88,166,255,.3); }
.grade-B { background:rgba(210,153,34,.15); color:var(--yellow); border:1px solid rgba(210,153,34,.3); }
.grade-C { background:rgba(139,148,158,.15); color:var(--text-muted); border:1px solid rgba(139,148,158,.3); }

/* Strategy badges */
.strategy-badge { display:inline-block; padding:2px 8px; border-radius:4px; font-weight:600; font-size:11px; }
.strategy-kuroten { background:rgba(63,185,80,.12); color:var(--green); }
.strategy-breakout { background:rgba(188,140,255,.12); color:var(--purple); }

/* Signal status badges */
.sig-badge { display:inline-block; padding:1px 6px; border-radius:3px; font-weight:600; font-size:10px; letter-spacing:.3px; }
.sig-new { background:rgba(88,166,255,.2); color:var(--accent); }
.sig-cont { background:rgba(139,148,158,.15); color:var(--text-dim); }
.sig-gone { background:rgba(248,81,73,.15); color:var(--red); text-decoration:line-through; }

/* Urgency badges */
.urgency { display:inline-block; padding:2px 8px; border-radius:4px; font-weight:700; font-size:11px; }
.urgency-HIGH { background:rgba(248,81,73,.15); color:var(--red); border:1px solid rgba(248,81,73,.3); }
.urgency-MEDIUM { background:rgba(210,153,34,.15); color:var(--yellow); border:1px solid rgba(210,153,34,.3); }
.urgency-LOW { background:rgba(139,148,158,.15); color:var(--text-muted); }

/* Sell alert banner */
.sell-alert { background:rgba(248,81,73,.08); border:1px solid rgba(248,81,73,.25); border-radius:8px; padding:14px 16px; margin-bottom:16px; }
.sell-alert h3 { font-size:14px; color:var(--red); margin-bottom:10px; display:flex; align-items:center; gap:6px; }
.sell-alert-item { display:flex; align-items:center; gap:10px; padding:6px 0; border-bottom:1px solid rgba(248,81,73,.1); font-size:13px; }
.sell-alert-item:last-child { border-bottom:none; }

/* Enriched detail row */
.enriched-row { display:flex; flex-wrap:wrap; gap:8px; margin-top:4px; }
.enriched-tag { display:inline-flex; align-items:center; gap:3px; padding:1px 6px; border-radius:3px; font-size:11px; background:var(--surface2); color:var(--text-muted); border:1px solid var(--border); }
.enriched-tag .etag-label { color:var(--text-dim); }

/* Signals */
.signal-day { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:14px 16px; margin-bottom:10px; }
.signal-day .day-header { font-weight:600; font-size:14px; margin-bottom:10px; display:flex; align-items:center; gap:10px; }
.signal-day .day-header .day-regime { font-size:11px; font-weight:400; }
.signal-section { margin-bottom:12px; }
.signal-section h4 { font-size:12px; color:var(--text-muted); margin-bottom:6px; padding-bottom:4px; border-bottom:1px solid var(--border); }
.signal-item { display:flex; align-items:flex-start; gap:10px; padding:5px 0; }
.signal-item .sig-code { font-weight:600; min-width:60px; }

/* Details */
.detail-card { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:16px; margin-bottom:12px; }
.detail-card h4 { font-size:15px; margin-bottom:8px; }
.detail-card .info-line { font-size:13px; color:var(--text-muted); margin:2px 0; }
.detail-card .links { margin-top:10px; font-size:13px; display:flex; gap:16px; }

/* Section headers */
.section-header { font-size:14px; font-weight:600; margin:20px 0 12px; padding-bottom:6px; border-bottom:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; }
.section-header .count { font-size:12px; color:var(--text-muted); font-weight:400; }

/* Gain/Loss coloring */
.gain { color:var(--green); }
.loss { color:var(--red); }
.neutral { color:var(--text-muted); }

/* Status indicators */
.status-dot { display:inline-block; width:7px; height:7px; border-radius:50%; margin-right:5px; }
.status-dot.active { background:var(--green); }
.status-dot.warning { background:var(--yellow); }
.status-dot.danger { background:var(--red); }

/* Empty state */
.empty { text-align:center; padding:48px 20px; color:var(--text-muted); font-size:14px; }

/* Performance breakdown */
.perf-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:20px; }
.perf-section { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:16px; }
.perf-section h4 { font-size:13px; color:var(--text-muted); margin-bottom:10px; }
.perf-row { display:flex; justify-content:space-between; padding:4px 0; font-size:13px; }
.perf-row .perf-label { color:var(--text-muted); }
.perf-row .perf-value { font-weight:600; font-family:'SF Mono','Cascadia Code','Consolas',monospace; }

/* Responsive */
@media (max-width:768px) {
  .cards { grid-template-columns:1fr 1fr; }
  .card .value { font-size:20px; }
  .perf-grid { grid-template-columns:1fr; }
  table { font-size:12px; }
  th, td { padding:5px 6px; }
}

/* Loading */
.loading { text-align:center; padding:60px; color:var(--text-muted); }
.spinner { display:inline-block; width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin .8s linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>
      inv_kuroten
      <span class="subtitle">黒字転換2倍株 投資システム</span>
    </h1>
    <div class="header-right">
      <span id="regime-badge" class="regime UNKNOWN">...</span>
      <div style="text-align:right;">
        <div class="date" id="today-date"></div>
        <div class="freshness" id="data-freshness"></div>
      </div>
    </div>
  </header>

  <div class="tabs">
    <div class="tab active" data-tab="overview">概況</div>
    <div class="tab" data-tab="watchlist">ウォッチリスト</div>
    <div class="tab" data-tab="portfolio">ポジション</div>
    <div class="tab" data-tab="signals">シグナル</div>
    <div class="tab" data-tab="performance">実績</div>
  </div>

  <!-- ========== 概況 ========== -->
  <div class="tab-content active" id="tab-overview">
    <div class="cards" id="overview-cards">
      <div class="loading"><div class="spinner"></div></div>
    </div>
    <div id="overview-sell-alerts"></div>
    <div class="section-header">ウォッチリスト<span class="count" id="wl-count"></span></div>
    <div id="overview-watchlist"><div class="loading"><div class="spinner"></div></div></div>
  </div>

  <!-- ========== ウォッチリスト ========== -->
  <div class="tab-content" id="tab-watchlist">
    <div id="watchlist-content"><div class="loading"><div class="spinner"></div></div></div>
  </div>

  <!-- ========== ポジション ========== -->
  <div class="tab-content" id="tab-portfolio">
    <div id="portfolio-content"><div class="loading"><div class="spinner"></div></div></div>
  </div>

  <!-- ========== シグナル ========== -->
  <div class="tab-content" id="tab-signals">
    <div id="signals-content"><div class="loading"><div class="spinner"></div></div></div>
  </div>

  <!-- ========== 実績 ========== -->
  <div class="tab-content" id="tab-performance">
    <div id="perf-cards"></div>
    <div id="perf-breakdown"></div>
    <div class="section-header">取引履歴<span class="count" id="trade-count"></span></div>
    <div id="perf-history"></div>
  </div>
</div>

<script>
// ─── 定数・翻訳マップ ───

const STRATEGY_NAMES = { kuroten: '黒字転換', breakout: 'ブレイクアウト' };
const RULE_NAMES = {
  stop_loss: '損切り', profit_target: '利確（目標到達）',
  trailing_stop: 'トレーリングストップ', deficit: '赤字転落',
  time_limit: '保有期限超過', partial_profit: '部分利確推奨',
};
const TREND_LABELS = { BULL: '強気', BEAR: '弱気', NEUTRAL: '中立', UNKNOWN: '不明' };
const TREND_ICONS  = { BULL: '\u25B2', BEAR: '\u25BC', NEUTRAL: '\u25CF', UNKNOWN: '?' };

// ─── ユーティリティ ───

const $ = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);

function fmtPct(n) { return n != null ? (n >= 0 ? '+' : '') + (n * 100).toFixed(1) + '%' : '\u2014'; }
function fmtYen(n) { return n != null ? '\u00A5' + Math.round(n).toLocaleString() : '\u2014'; }
function fmtUSD(n) { return n != null ? '$' + n.toFixed(2) : '\u2014'; }
function fmtPrice(n, market) { return market === 'US' ? fmtUSD(n) : fmtYen(n); }
function fmtNum(n, d) { return n != null ? n.toFixed(d != null ? d : 1) : '\u2014'; }
function fmtDate(s) { return s ? s.replace(/-/g, '/') : '\u2014'; }
function gainClass(n) { return n > 0.001 ? 'gain' : n < -0.001 ? 'loss' : 'neutral'; }
function strategyName(s) { return STRATEGY_NAMES[s] || s; }
function strategyBadge(s) { return '<span class="strategy-badge strategy-' + s + '">' + strategyName(s) + '</span>'; }
function gradeHTML(g) { return g ? '<span class="grade grade-' + g + '">' + g + '</span>' : ''; }
function urgencyBadge(u) { return '<span class="urgency urgency-' + u + '">' + (u === 'HIGH' ? '\u8981\u5BFE\u5FDC' : u === 'MEDIUM' ? '\u8981\u6CE8\u610F' : u) + '</span>'; }
function ruleName(r) { return RULE_NAMES[r] || r; }

function fmtSignalKey(key) {
  const parts = key.split(':');
  const strategy = parts[0], market = parts[1];
  const name = STRATEGY_NAMES[strategy] || strategy;
  const mkt = market === 'US' ? '\u7C73\u56FD\u682A' : market === 'JP' ? '\u65E5\u672C\u682A' : market;
  return name + '\uFF08' + mkt + '\uFF09';
}

function daysBetween(dateStr) {
  if (!dateStr) return 0;
  return Math.round((Date.now() - new Date(dateStr).getTime()) / 86400000);
}

// ─── タブ切り替え ───

$$('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    $$('.tab').forEach(t => t.classList.remove('active'));
    $$('.tab-content').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    $('#tab-' + tab.dataset.tab).classList.add('active');
  });
});

// ─── データ取得 ───

async function fetchJSON(url) { return (await fetch(url)).json(); }

async function loadAll() {
  const [regime, portfolio, watchlist, signals, stats, history] = await Promise.all([
    fetchJSON('/api/regime'), fetchJSON('/api/portfolio'),
    fetchJSON('/api/watchlist'), fetchJSON('/api/signals'),
    fetchJSON('/api/stats'), fetchJSON('/api/history'),
  ]);
  renderHeader(regime, signals);
  renderOverview(regime, portfolio, watchlist, stats, signals);
  renderWatchlist(watchlist);
  renderPortfolio(portfolio);
  renderSignals(signals);
  renderPerformance(stats, history);
}

// ─── ヘッダー ───

function renderHeader(r, signals) {
  const badge = $('#regime-badge');
  badge.className = 'regime ' + r.trend;
  badge.innerHTML = TREND_ICONS[r.trend] + ' ' + TREND_LABELS[r.trend];
  badge.title = r.description + '\n\u65E5\u7D4C225: ' + fmtYen(r.price)
    + '\nSMA50: ' + fmtYen(r.sma50) + '\nSMA200: ' + fmtYen(r.sma200);
  $('#today-date').textContent = new Date().toLocaleDateString('ja-JP', {year:'numeric',month:'long',day:'numeric'});
  // Data freshness
  if (signals && signals.length > 0) {
    const latest = signals[0].date;
    if (latest) {
      const days = daysBetween(latest);
      $('#data-freshness').textContent = '\u6700\u7D42\u66F4\u65B0: ' + fmtDate(latest)
        + (days === 0 ? ' (\u672C\u65E5)' : days === 1 ? ' (1\u65E5\u524D)' : ' (' + days + '\u65E5\u524D)');
    }
  }
}

// ─── 概況 ───

function renderOverview(regime, portfolio, watchlist, stats, signals) {
  const positions = Object.values(portfolio);
  const posCount = positions.length;
  const jpCount = positions.filter(p => p.market !== 'US').length;
  const usCount = positions.filter(p => p.market === 'US').length;
  const kuroCount = positions.filter(p => p.strategy === 'kuroten').length;
  const boCount = positions.filter(p => p.strategy === 'breakout').length;

  const regimeDetail = regime.price
    ? '\u65E5\u7D4C225: ' + fmtYen(regime.price) + '<br>SMA50: ' + fmtYen(regime.sma50) + ' / SMA200: ' + fmtYen(regime.sma200)
    : '\u2014';

  let cards = ''
    + '<div class="card">'
    + '  <div class="label">\u76F8\u5834\u74B0\u5883</div>'
    + '  <div class="value" style="font-size:18px;">' + (regime.description || TREND_LABELS[regime.trend]) + '</div>'
    + '  <div class="sub">' + regimeDetail + '</div>'
    + '</div>'
    + '<div class="card">'
    + '  <div class="label">\u4FDD\u6709\u30DD\u30B8\u30B7\u30E7\u30F3</div>'
    + '  <div class="value">' + posCount + '<span style="font-size:14px;color:var(--text-muted);font-weight:400;">\u9298\u67C4</span></div>'
    + '  <div class="sub">\u9ED2\u5B57\u8EE2\u63DB ' + kuroCount + ' / \u30D6\u30EC\u30A4\u30AF\u30A2\u30A6\u30C8 ' + boCount + (usCount > 0 ? '<br>JP ' + jpCount + ' / US ' + usCount : '') + '</div>'
    + '</div>'
    + '<div class="card">'
    + '  <div class="label">\u30A6\u30A9\u30C3\u30C1\u30EA\u30B9\u30C8</div>'
    + '  <div class="value">' + (watchlist.stocks?.length || 0) + '<span style="font-size:14px;color:var(--text-muted);font-weight:400;">\u9298\u67C4</span></div>'
    + '  <div class="sub">' + (watchlist.title || '\u2014') + '</div>'
    + '</div>'
    + '<div class="card">'
    + '  <div class="label">\u52DD\u7387</div>'
    + '  <div class="value ' + (stats.total_trades > 0 ? gainClass(stats.win_rate - 0.5) : '') + '">' + (stats.total_trades > 0 ? (stats.win_rate*100).toFixed(0) + '%' : '\u2014') + '</div>'
    + '  <div class="sub">' + stats.wins + '\u52DD ' + stats.losses + '\u6557\uFF08\u8A08' + stats.total_trades + '\u6226\uFF09</div>'
    + '</div>'
    + '<div class="card">'
    + '  <div class="label">\u7D2F\u8A08\u640D\u76CA\uFF08\u5B9F\u73FE\uFF09</div>'
    + '  <div class="value ' + gainClass(stats.total_profit) + '">' + (stats.total_trades > 0 ? fmtYen(stats.total_profit) : '\u2014') + '</div>'
    + '  <div class="sub">PF: ' + (stats.profit_factor || '\u2014') + ' / \u5E73\u5747\u640D\u76CA\u7387: ' + (stats.total_trades > 0 ? fmtPct(stats.avg_return) : '\u2014') + '</div>'
    + '</div>'
    + '<div class="card">'
    + '  <div class="label">\u5E73\u5747\u4FDD\u6709\u671F\u9593</div>'
    + '  <div class="value">' + (stats.avg_hold_days ? Math.round(stats.avg_hold_days) + '<span style="font-size:14px;color:var(--text-muted);font-weight:400;">\u65E5</span>' : '\u2014') + '</div>'
    + '  <div class="sub">\u6700\u5927\u9023\u52DD: ' + stats.consecutive_wins + ' / \u6700\u5927\u9023\u6557: ' + stats.consecutive_losses + '</div>'
    + '</div>';
  $('#overview-cards').innerHTML = cards;

  // 売却シグナル（直近シグナルから取得）
  const latestSells = (signals && signals.length > 0) ? (signals[0].sell_signals || []) : [];
  if (latestSells.length > 0) {
    let html = '<div class="sell-alert">';
    html += '<h3>\u26A0 \u58F2\u5374\u30B7\u30B0\u30CA\u30EB\u691C\u51FA\uFF08' + fmtDate(signals[0].date) + '\uFF09\u2014 ' + latestSells.length + '\u4EF6</h3>';
    latestSells.forEach(s => {
      const ret = s.return_pct != null ? fmtPct(s.return_pct) : '';
      html += '<div class="sell-alert-item">'
        + urgencyBadge(s.urgency)
        + ' <strong>' + s.code + '</strong>'
        + ' <span>' + strategyBadge(s.strategy) + '</span>'
        + ' <span>' + ruleName(s.rule) + '</span>'
        + ' <span class="mono ' + gainClass(s.return_pct) + '">' + ret + '</span>'
        + ' <span style="color:var(--text-muted);font-size:12px;">' + (s.message || '') + '</span>'
        + '</div>';
    });
    html += '</div>';
    $('#overview-sell-alerts').innerHTML = html;
  } else {
    $('#overview-sell-alerts').innerHTML = '';
  }

  // ウォッチリスト
  $('#wl-count').textContent = watchlist.stocks?.length ? '(' + watchlist.stocks.length + '\u9298\u67C4)' : '';
  if (watchlist.stocks?.length) {
    let html = '<table>'
      + '<tr><th>\u63A8\u5968\u5EA6</th><th>\u30B3\u30FC\u30C9</th><th>\u9298\u67C4\u540D</th>'
      + '<th class="right">\u682A\u4FA1</th><th class="right">\u6642\u4FA1\u7DCF\u984D</th>'
      + '<th>\u63A8\u5968\u7406\u7531</th></tr>';
    watchlist.stocks.forEach(s => {
      const grade = (s['\u63A8\u5968'] || '').replace(/\*/g, '');
      const code = s['\u30B3\u30FC\u30C9'] || '';
      html += '<tr>'
        + '<td class="center">' + gradeHTML(grade) + '</td>'
        + '<td><a href="https://irbank.net/' + code + '" target="_blank"><strong>' + code + '</strong></a></td>'
        + '<td>' + (s['\u9298\u67C4\u540D'] || '') + '</td>'
        + '<td class="right mono">' + (s['\u682A\u4FA1(\u5186)'] || '') + '</td>'
        + '<td class="right mono">' + (s['\u6642\u4FA1\u7DCF\u984D(\u5104\u5186)'] || '') + '\u5104</td>'
        + '<td style="font-size:12px;color:var(--text-muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + (s['\u63A8\u5968\u7406\u7531'] || '') + '</td>'
        + '</tr>';
    });
    html += '</table>';
    $('#overview-watchlist').innerHTML = html;
  } else {
    $('#overview-watchlist').innerHTML = '<div class="empty">\u30A6\u30A9\u30C3\u30C1\u30EA\u30B9\u30C8\u306A\u3057</div>';
  }
}

// ─── ウォッチリスト ───

function renderWatchlist(wl) {
  if (!wl.stocks?.length) {
    $('#watchlist-content').innerHTML = '<div class="empty">\u30A6\u30A9\u30C3\u30C1\u30EA\u30B9\u30C8\u306A\u3057</div>';
    return;
  }
  let html = '<div class="section-header">' + wl.title + '</div>';

  if (wl.details?.length) {
    wl.details.forEach(d => {
      const code = d.header.split(' ')[0];
      const gradeMatch = d.header.match(/\[(S|A|B|C)\]/);
      const grade = gradeMatch ? gradeMatch[1] : '';
      html += '<div class="detail-card">'
        + '<h4>' + gradeHTML(grade) + ' ' + d.header.replace(/\[.\]/, '') + '</h4>'
        + d.lines.map(l => {
          let cls = '';
          if (l.includes('\u9ED2\u5B57\u8EE2\u63DB')) cls = 'gain';
          if (l.includes('\u30D5\u30A7\u30A4\u30AF')) cls = 'loss';
          return '<div class="info-line ' + cls + '">' + l + '</div>';
        }).join('')
        + '<div class="links">'
        + '<a href="https://irbank.net/' + code + '" target="_blank">IR Bank</a>'
        + '<a href="https://finance.yahoo.co.jp/quote/' + code + '.T" target="_blank">Yahoo\u30D5\u30A1\u30A4\u30CA\u30F3\u30B9</a>'
        + '<a href="https://monex.ifis.co.jp/index.php?sa=report_zaimu&bcode=' + code + '" target="_blank">\u9298\u67C4\u30B9\u30AB\u30A6\u30BF\u30FC</a>'
        + '</div>'
        + '</div>';
    });
  } else {
    html += '<table>'
      + '<tr><th>\u63A8\u5968\u5EA6</th><th>\u30B3\u30FC\u30C9</th><th>\u9298\u67C4\u540D</th>'
      + '<th class="right">\u682A\u4FA1</th><th class="right">\u6642\u4FA1\u7DCF\u984D</th>'
      + '<th class="right">\u55B6\u696D\u5229\u76CA</th><th>\u30D5\u30A7\u30A4\u30AF\u30B9\u30B3\u30A2</th>'
      + '<th>\u63A8\u5968\u7406\u7531</th></tr>';
    wl.stocks.forEach(s => {
      const grade = (s['\u63A8\u5968'] || '').replace(/\*/g, '');
      html += '<tr>'
        + '<td class="center">' + gradeHTML(grade) + '</td>'
        + '<td>' + (s['\u30B3\u30FC\u30C9'] || '') + '</td>'
        + '<td>' + (s['\u9298\u67C4\u540D'] || '') + '</td>'
        + '<td class="right mono">' + (s['\u682A\u4FA1(\u5186)'] || '') + '</td>'
        + '<td class="right mono">' + (s['\u6642\u4FA1\u7DCF\u984D(\u5104\u5186)'] || '') + '\u5104</td>'
        + '<td class="right mono">' + (s['\u55B6\u696D\u5229\u76CA(\u5104\u5186)'] || '') + '\u5104</td>'
        + '<td>' + (s['\u30D5\u30A7\u30A4\u30AF'] || '') + '</td>'
        + '<td style="font-size:12px">' + (s['\u63A8\u5968\u7406\u7531'] || '') + '</td>'
        + '</tr>';
    });
    html += '</table>';
  }
  $('#watchlist-content').innerHTML = html;
}

// ─── ポジション ───

function renderPortfolio(portfolio) {
  const positions = Object.values(portfolio);
  if (!positions.length) {
    $('#portfolio-content').innerHTML = '<div class="empty">\u4FDD\u6709\u30DD\u30B8\u30B7\u30E7\u30F3\u306A\u3057</div>';
    return;
  }

  // Sort: trailing_active first, then by buy_date desc
  positions.sort((a, b) => {
    if (a.trailing_active !== b.trailing_active) return a.trailing_active ? -1 : 1;
    return (b.buy_date || '').localeCompare(a.buy_date || '');
  });

  let html = '<table>'
    + '<tr><th>\u9298\u67C4</th><th>\u6226\u7565</th><th>\u5E02\u5834</th>'
    + '<th class="right">\u8CB7\u4ED8\u5358\u4FA1</th><th class="right">\u682A\u6570</th>'
    + '<th>\u8CB7\u4ED8\u65E5</th><th class="right">\u6700\u9AD8\u5024\u5230\u9054</th>'
    + '<th>\u30C8\u30EC\u30FC\u30EA\u30F3\u30B0</th><th>\u90E8\u5206\u5229\u78BA</th>'
    + '<th class="right">\u4FDD\u6709\u65E5\u6570</th><th>\u5099\u8003</th></tr>';

  positions.forEach(p => {
    const days = daysBetween(p.buy_date);
    const peakGain = (p.peak_price && p.buy_price) ? (p.peak_price - p.buy_price) / p.buy_price : 0;
    const daysWarn = days > 600 ? 'loss' : days > 500 ? 'neutral' : '';
    const trailStatus = p.trailing_active
      ? '<span class="status-dot warning"></span>\u767A\u52D5\u4E2D'
      : '<span class="status-dot"></span>\u672A\u767A\u52D5';
    const partialStatus = p.partial_sold
      ? '<span class="gain">\u6E08 (' + fmtDate(p.partial_sell_date) + ')</span>'
      : '<span style="color:var(--text-dim);">\u2014</span>';

    // Entry context
    let contextNote = p.notes || '';
    if (p.signal_context) {
      const ctx = p.signal_context;
      const parts = [];
      if (ctx.grade) parts.push(ctx.grade + '\u8A55\u4FA1');
      if (ctx.regime) parts.push(ctx.regime);
      if (ctx.rs_score) parts.push('RS:' + ctx.rs_score);
      if (ctx.ea_tag) parts.push(ctx.ea_tag);
      if (parts.length) contextNote = parts.join(' / ') + (contextNote ? ' | ' + contextNote : '');
    }

    html += '<tr>'
      + '<td><strong>' + p.code + '</strong></td>'
      + '<td>' + strategyBadge(p.strategy) + '</td>'
      + '<td>' + p.market + '</td>'
      + '<td class="right mono">' + fmtPrice(p.buy_price, p.market) + '</td>'
      + '<td class="right mono">' + (p.shares || 0).toLocaleString() + '</td>'
      + '<td>' + fmtDate(p.buy_date) + '</td>'
      + '<td class="right"><span class="' + gainClass(peakGain) + ' mono">' + fmtPct(peakGain) + '</span></td>'
      + '<td>' + trailStatus + '</td>'
      + '<td>' + partialStatus + '</td>'
      + '<td class="right ' + daysWarn + '">' + days + '\u65E5</td>'
      + '<td style="font-size:12px;color:var(--text-muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + contextNote + '</td>'
      + '</tr>';
  });
  html += '</table>';
  $('#portfolio-content').innerHTML = html;
}

// ─── シグナル ───

function renderSignals(signals) {
  if (!signals?.length) {
    $('#signals-content').innerHTML = '<div class="empty">\u76F4\u8FD1\u306E\u30B7\u30B0\u30CA\u30EB\u306A\u3057</div>';
    return;
  }

  let html = '';
  signals.forEach((day, idx) => {
    const sigs = day.signals || {};
    const enriched = day.enriched || {};
    const sellSigs = day.sell_signals || [];
    const regime = day.regime;
    const allCodes = Object.entries(sigs).flatMap(([key, codes]) =>
      (codes || []).map(c => ({ key, code: c }))
    );
    const totalCount = allCodes.length + sellSigs.length;

    html += '<div class="signal-day">';
    html += '<div class="day-header">'
      + fmtDate(day.date)
      + ' <span style="color:var(--text-muted);font-weight:400;font-size:13px;">('
      + (allCodes.length > 0 ? '\u8CB7\u3044' + allCodes.length + '\u4EF6' : '')
      + (allCodes.length > 0 && sellSigs.length > 0 ? ' / ' : '')
      + (sellSigs.length > 0 ? '\u58F2\u308A' + sellSigs.length + '\u4EF6' : '')
      + (totalCount === 0 ? '\u691C\u51FA\u306A\u3057' : '')
      + ')</span>';
    if (regime) {
      html += ' <span class="day-regime regime ' + regime.trend + '" style="font-size:11px;padding:2px 8px;">'
        + TREND_ICONS[regime.trend] + ' ' + TREND_LABELS[regime.trend] + '</span>';
    }
    html += '</div>';

    // 売却シグナル
    if (sellSigs.length > 0) {
      html += '<div class="signal-section">';
      html += '<h4>\u58F2\u5374\u30B7\u30B0\u30CA\u30EB</h4>';
      sellSigs.forEach(s => {
        html += '<div class="signal-item">'
          + urgencyBadge(s.urgency)
          + ' <span class="sig-code">' + s.code + '</span>'
          + ' ' + strategyBadge(s.strategy)
          + ' <span>' + ruleName(s.rule) + '</span>'
          + ' <span class="mono ' + gainClass(s.return_pct) + '">' + fmtPct(s.return_pct) + '</span>'
          + ' <span style="color:var(--text-muted);font-size:12px;">\u4FDD\u6709' + (s.hold_days || 0) + '\u65E5</span>'
          + '</div>';
      });
      html += '</div>';
    }

    // 買いシグナル（戦略別）
    const keys = Object.keys(sigs).filter(k => (sigs[k] || []).length > 0);
    if (keys.length > 0) {
      keys.sort().forEach(key => {
        const codes = sigs[key] || [];
        const enrichedList = enriched[key] || [];
        const enrichedMap = {};
        enrichedList.forEach(e => { enrichedMap[e.code] = e; });

        html += '<div class="signal-section">';
        html += '<h4>' + fmtSignalKey(key) + ' \u2014 ' + codes.length + '\u9298\u67C4</h4>';

        codes.forEach(code => {
          const e = enrichedMap[code];
          const link = key.includes('US')
            ? 'https://finance.yahoo.com/quote/' + code
            : 'https://irbank.net/' + code;

          html += '<div class="signal-item">'
            + '<a href="' + link + '" target="_blank" class="sig-code">' + code + '</a>';

          if (e) {
            // Enriched data display
            if (e.name) html += ' <span style="color:var(--text-muted);font-size:12px;">' + e.name + '</span>';
            html += '<div class="enriched-row">';
            if (e.close != null) html += '<span class="enriched-tag"><span class="etag-label">\u7D42\u5024</span>' + fmtNum(e.close, 0) + '</span>';
            if (e.rs_score != null) html += '<span class="enriched-tag"><span class="etag-label">RS</span>' + Math.round(e.rs_score) + '</span>';
            if (e.volume_ratio != null) html += '<span class="enriched-tag"><span class="etag-label">\u51FA\u6765\u9AD8\u6BD4</span>' + fmtNum(e.volume_ratio, 1) + 'x</span>';
            if (e.ea_tag) html += '<span class="enriched-tag" style="color:var(--green);">' + e.ea_tag + '</span>';
            if (e.gc_status === true || e.gc_status === 'True') html += '<span class="enriched-tag" style="color:var(--yellow);">GC\u6E08</span>';
            if (e.rsi != null) html += '<span class="enriched-tag"><span class="etag-label">RSI</span>' + Math.round(e.rsi) + '</span>';
            if (e.distance_pct != null) html += '<span class="enriched-tag"><span class="etag-label">52W\u9AD8\u5024\u6BD4</span>' + fmtPct(e.distance_pct / 100) + '</span>';
            if (e.market_cap != null) {
              const mcap = key.includes('US') ? '$' + (e.market_cap / 1e9).toFixed(1) + 'B' : Math.round(e.market_cap / 1e8) + '\u5104';
              html += '<span class="enriched-tag"><span class="etag-label">\u6642\u4FA1\u7DCF\u984D</span>' + mcap + '</span>';
            }
            if (e.sector) html += '<span class="enriched-tag"><span class="etag-label">\u30BB\u30AF\u30BF\u30FC</span>' + e.sector + '</span>';
            html += '</div>';
          }
          html += '</div>';
        });
        html += '</div>';
      });
    }

    if (totalCount === 0) {
      html += '<div style="color:var(--text-muted);font-size:13px;">\u30B7\u30B0\u30CA\u30EB\u691C\u51FA\u306A\u3057</div>';
    }
    html += '</div>';
  });

  $('#signals-content').innerHTML = html;
}

// ─── 実績 ───

function renderPerformance(stats, history) {
  // メトリクスカード
  let cards = '<div class="cards">'
    + '<div class="card"><div class="label">\u7DCF\u53D6\u5F15\u6570</div>'
    + '<div class="value">' + stats.total_trades + '</div></div>'
    + '<div class="card"><div class="label">\u52DD\u7387</div>'
    + '<div class="value ' + (stats.total_trades > 0 ? gainClass(stats.win_rate - 0.5) : '') + '">' + (stats.total_trades > 0 ? (stats.win_rate*100).toFixed(0) + '%' : '\u2014') + '</div>'
    + '<div class="sub">' + stats.wins + '\u52DD ' + stats.losses + '\u6557</div></div>'
    + '<div class="card"><div class="label">\u5E73\u5747\u640D\u76CA\u7387</div>'
    + '<div class="value ' + gainClass(stats.avg_return) + '">' + (stats.total_trades > 0 ? fmtPct(stats.avg_return) : '\u2014') + '</div></div>'
    + '<div class="card"><div class="label">PF\uFF08\u30D7\u30ED\u30D5\u30A3\u30C3\u30C8\u30D5\u30A1\u30AF\u30BF\u30FC\uFF09</div>'
    + '<div class="value">' + (stats.profit_factor || '\u2014') + '</div></div>'
    + '<div class="card"><div class="label">\u7D2F\u8A08\u640D\u76CA</div>'
    + '<div class="value ' + gainClass(stats.total_profit) + '">' + (stats.total_trades > 0 ? fmtYen(stats.total_profit) : '\u2014') + '</div></div>'
    + '<div class="card"><div class="label">\u5E73\u5747\u4FDD\u6709\u671F\u9593</div>'
    + '<div class="value">' + (stats.avg_hold_days ? Math.round(stats.avg_hold_days) + '\u65E5' : '\u2014') + '</div></div>'
    + '</div>';
  $('#perf-cards').innerHTML = cards;

  // 詳細ブレークダウン
  if (stats.total_trades > 0) {
    let bd = '<div class="perf-grid">'
      + '<div class="perf-section"><h4>\u52DD\u3061\u30C8\u30EC\u30FC\u30C9</h4>'
      + '<div class="perf-row"><span class="perf-label">\u5E73\u5747\u5229\u76CA\u7387</span><span class="perf-value gain">' + fmtPct(stats.avg_win) + '</span></div>'
      + '<div class="perf-row"><span class="perf-label">\u6700\u5927\u5229\u76CA\u7387</span><span class="perf-value gain">' + fmtPct(stats.max_win) + '</span></div>'
      + '<div class="perf-row"><span class="perf-label">\u6700\u5927\u9023\u52DD</span><span class="perf-value">' + stats.consecutive_wins + '\u56DE</span></div>'
      + '</div>'
      + '<div class="perf-section"><h4>\u8CA0\u3051\u30C8\u30EC\u30FC\u30C9</h4>'
      + '<div class="perf-row"><span class="perf-label">\u5E73\u5747\u640D\u5931\u7387</span><span class="perf-value loss">' + fmtPct(stats.avg_loss) + '</span></div>'
      + '<div class="perf-row"><span class="perf-label">\u6700\u5927\u640D\u5931\u7387</span><span class="perf-value loss">' + fmtPct(stats.max_loss) + '</span></div>'
      + '<div class="perf-row"><span class="perf-label">\u6700\u5927\u9023\u6557</span><span class="perf-value">' + stats.consecutive_losses + '\u56DE</span></div>'
      + '</div>'
      + '</div>';
    $('#perf-breakdown').innerHTML = bd;
  }

  // 取引履歴
  $('#trade-count').textContent = history?.length ? '(' + history.length + '\u4EF6)' : '';
  if (!history?.length) {
    $('#perf-history').innerHTML = '<div class="empty">\u53D6\u5F15\u5C65\u6B74\u306A\u3057</div>';
    return;
  }

  let html = '<table>'
    + '<tr><th>\u9298\u67C4</th><th>\u6226\u7565</th><th>\u5E02\u5834</th>'
    + '<th>\u8CB7\u4ED8\u65E5</th><th>\u58F2\u5374\u65E5</th>'
    + '<th class="right">\u640D\u76CA\u7387</th><th class="right">\u640D\u76CA\u984D</th>'
    + '<th class="right">\u4FDD\u6709\u65E5\u6570</th><th>\u58F2\u5374\u7406\u7531</th></tr>';
  history.slice().reverse().forEach(t => {
    html += '<tr>'
      + '<td><strong>' + t.code + '</strong></td>'
      + '<td>' + strategyBadge(t.strategy) + '</td>'
      + '<td>' + (t.market || 'JP') + '</td>'
      + '<td>' + fmtDate(t.buy_date) + '</td>'
      + '<td>' + fmtDate(t.sell_date) + '</td>'
      + '<td class="right mono ' + gainClass(t.return_pct) + '">' + fmtPct(t.return_pct) + '</td>'
      + '<td class="right mono ' + gainClass(t.profit) + '">' + fmtYen(t.profit) + '</td>'
      + '<td class="right">' + (t.hold_days || 0) + '\u65E5</td>'
      + '<td style="font-size:12px;color:var(--text-muted);">' + (t.sell_reason || '') + '</td>'
      + '</tr>';
  });
  html += '</table>';
  $('#perf-history').innerHTML = html;
}

// ─── 起動 ───
loadAll();
</script>
</body>
</html>
"""


# ─── HTTP Server ───


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/"):
            status, data = handle_api(path)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False, default=str).encode("utf-8"))
        elif path == "/favicon.svg":
            self.send_response(200)
            self.send_header("Content-Type", "image/svg+xml")
            self.end_headers()
            self.wfile.write(FAVICON_SVG.encode("utf-8"))
        elif path == "/" or path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress noisy request logs
        pass


def main():
    parser = argparse.ArgumentParser(description="inv_kuroten Dashboard")
    parser.add_argument("--port", type=int, default=8501)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), DashboardHandler)
    url = f"http://{args.host}:{args.port}"
    print(f"Dashboard running at {url}")
    print("Press Ctrl+C to stop")

    try:
        import webbrowser
        webbrowser.open(url)
    except Exception:
        pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutdown.")
        server.server_close()


if __name__ == "__main__":
    main()
