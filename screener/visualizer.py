"""
バックテスト結果の可視化モジュール

matplotlib を使用してバックテスト結果をチャートとして出力する。
- 累積損益曲線
- リターン分布ヒストグラム
- 推奨度別パフォーマンス棒グラフ
"""

import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # 非GUIバックエンド
import matplotlib.pyplot as plt
import numpy as np

# --- フォント設定（Windows: MS Gothic、フォールバック: デフォルト）---
_FONT_CANDIDATES = ["MS Gothic", "Yu Gothic", "Meiryo", "IPAGothic"]
_font_set = False
for _font in _FONT_CANDIDATES:
    try:
        from matplotlib.font_manager import FontProperties
        fp = FontProperties(family=_font)
        if fp.get_name() != _font:
            continue
        plt.rcParams["font.family"] = _font
        _font_set = True
        break
    except Exception:
        continue

if not _font_set:
    # フォールバック: sans-serif にMS Gothicを追加して試行
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.sans-serif"] = _FONT_CANDIDATES + plt.rcParams.get(
        "font.sans-serif", []
    )

plt.rcParams["axes.unicode_minus"] = False


# 投資単位（backtest.pyと同じデフォルト値）
_DEFAULT_PER_TRADE = 1_000_000


def plot_cumulative_pnl(trades: list[dict], output_path: str) -> None:
    """
    累積損益曲線を描画して保存する。

    Args:
        trades: バックテスト結果の取引リスト（各dictにreturn_pct, buy_dateが必要）
        output_path: 出力PNGファイルパス
    """
    if not trades:
        return

    # buy_dateでソート
    sorted_trades = sorted(trades, key=lambda t: t.get("buy_date", ""))
    cumulative = []
    running = 0.0
    for t in sorted_trades:
        pnl = _DEFAULT_PER_TRADE * t["return_pct"] / 100
        running += pnl
        cumulative.append(running)

    fig, ax = plt.subplots(figsize=(10, 5))
    x = list(range(1, len(cumulative) + 1))
    ax.plot(x, cumulative, linewidth=1.5, color="#2196F3")
    ax.fill_between(
        x,
        cumulative,
        0,
        where=[v >= 0 for v in cumulative],
        alpha=0.15,
        color="#2196F3",
    )
    ax.fill_between(
        x,
        cumulative,
        0,
        where=[v < 0 for v in cumulative],
        alpha=0.15,
        color="#F44336",
    )
    ax.axhline(y=0, color="gray", linewidth=0.8, linestyle="--")
    ax.set_title("累積損益曲線", fontsize=14)
    ax.set_xlabel("取引番号", fontsize=11)
    ax.set_ylabel("累積損益 (円)", fontsize=11)
    ax.grid(True, alpha=0.3)

    # Y軸に万円単位のフォーマット
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, _: f"{v:,.0f}")
    )

    fig.tight_layout()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_return_distribution(trades: list[dict], output_path: str) -> None:
    """
    リターン分布のヒストグラムを描画して保存する。

    Args:
        trades: バックテスト結果の取引リスト（各dictにreturn_pctが必要）
        output_path: 出力PNGファイルパス
    """
    if not trades:
        return

    returns = [t["return_pct"] for t in trades]
    mean_val = np.mean(returns)
    median_val = np.median(returns)

    fig, ax = plt.subplots(figsize=(10, 5))

    # ビン数の自動調整
    n_bins = min(max(len(returns) // 3, 10), 50)

    ax.hist(returns, bins=n_bins, color="#4CAF50", alpha=0.7, edgecolor="white")
    ax.axvline(mean_val, color="#F44336", linewidth=2, linestyle="--",
               label=f"平均: {mean_val:+.1f}%")
    ax.axvline(median_val, color="#FF9800", linewidth=2, linestyle="-.",
               label=f"中央値: {median_val:+.1f}%")
    ax.axvline(0, color="gray", linewidth=0.8, linestyle=":")

    ax.set_title("リターン分布", fontsize=14)
    ax.set_xlabel("リターン (%)", fontsize=11)
    ax.set_ylabel("取引数", fontsize=11)
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    fig.tight_layout()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_grade_performance(trades: list[dict], output_path: str) -> None:
    """
    推奨度別パフォーマンス（勝率・平均リターン）の棒グラフを描画して保存する。

    Args:
        trades: バックテスト結果の取引リスト（各dictにrec_grade or grade, return_pctが必要）
        output_path: 出力PNGファイルパス
    """
    if not trades:
        return

    grades_order = ["S", "A", "B", "C"]
    grade_data = {g: [] for g in grades_order}

    for t in trades:
        g = t.get("grade") or t.get("rec_grade")
        if g in grade_data:
            grade_data[g].append(t["return_pct"])

    # 存在するグレードだけ表示
    labels = []
    win_rates = []
    avg_returns = []
    counts = []
    for g in grades_order:
        rets = grade_data[g]
        if not rets:
            continue
        labels.append(g)
        wins = sum(1 for r in rets if r > 0)
        win_rates.append(wins / len(rets) * 100)
        avg_returns.append(np.mean(rets))
        counts.append(len(rets))

    if not labels:
        return

    x = np.arange(len(labels))
    width = 0.35

    fig, ax1 = plt.subplots(figsize=(8, 5))

    bars1 = ax1.bar(x - width / 2, win_rates, width, label="勝率 (%)",
                    color="#2196F3", alpha=0.8)
    ax1.set_ylabel("勝率 (%)", fontsize=11, color="#2196F3")
    ax1.set_ylim(0, 100)
    ax1.tick_params(axis="y", labelcolor="#2196F3")

    # 勝率バーの上に値ラベル
    for bar, wr in zip(bars1, win_rates):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                 f"{wr:.0f}%", ha="center", va="bottom", fontsize=9,
                 color="#2196F3")

    ax2 = ax1.twinx()
    bars2 = ax2.bar(x + width / 2, avg_returns, width, label="平均リターン (%)",
                    color="#FF9800", alpha=0.8)
    ax2.set_ylabel("平均リターン (%)", fontsize=11, color="#FF9800")
    ax2.tick_params(axis="y", labelcolor="#FF9800")
    ax2.axhline(y=0, color="gray", linewidth=0.5, linestyle="--")

    # 平均リターンバーの上に値ラベル
    for bar, ar in zip(bars2, avg_returns):
        y_pos = bar.get_height() if ar >= 0 else bar.get_height() - 2
        va = "bottom" if ar >= 0 else "top"
        ax2.text(bar.get_x() + bar.get_width() / 2, y_pos,
                 f"{ar:+.1f}%", ha="center", va=va, fontsize=9,
                 color="#FF9800")

    ax1.set_xlabel("推奨度", fontsize=11)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{lbl}\n(n={c})" for lbl, c in zip(labels, counts)],
                        fontsize=10)
    ax1.set_title("推奨度別パフォーマンス", fontsize=14)
    ax1.grid(True, axis="y", alpha=0.3)

    # 凡例を結合
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=10)

    fig.tight_layout()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def generate_all_charts(trades: list[dict], output_dir: str) -> None:
    """
    全チャートを生成してPNGとして保存する。

    Args:
        trades: バックテスト結果の取引リスト
        output_dir: 出力ディレクトリパス
    """
    if not trades:
        return

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"\nチャート生成中...")

    plot_cumulative_pnl(trades, str(out / "cumulative_pnl.png"))
    print(f"  累積損益曲線: {out / 'cumulative_pnl.png'}")

    plot_return_distribution(trades, str(out / "return_distribution.png"))
    print(f"  リターン分布: {out / 'return_distribution.png'}")

    plot_grade_performance(trades, str(out / "grade_performance.png"))
    print(f"  推奨度別パフォーマンス: {out / 'grade_performance.png'}")

    print(f"  チャート保存完了: {out}")
