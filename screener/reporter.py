"""
ウォッチリスト生成
スクリーニング結果をMarkdownファイルとして出力する
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "watchlist"


def _quarter_label(date_str: str) -> str:
    """日付文字列 (YYYYMMDD) から四半期ラベルを返す (例: 2026-Q1)"""
    dt = datetime.strptime(date_str, "%Y%m%d")
    quarter = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{quarter}"


def generate_watchlist(df: pd.DataFrame, date: str) -> str:
    """
    ウォッチリストをMarkdownファイルとして生成する

    Args:
        df: フィルタ済みのDataFrame
        date: 対象日付 (YYYYMMDD)

    Returns:
        出力ファイルパス
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    label = _quarter_label(date)
    output_path = DATA_DIR / f"{label}.md"

    has_fake = "fake_flags" in df.columns if not df.empty else False
    has_rec = "Recommendation" in df.columns if not df.empty else False

    lines = [
        f"# 黒字転換ウォッチリスト {label}",
        f"",
        f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"対象日付: {date}",
        f"該当銘柄数: {len(df)} 件",
        f"",
    ]

    # 推奨度の説明
    if has_rec:
        lines.extend([
            "## 推奨度について",
            "- **S**: 最有力候補。長期赤字からの復活+大きな転換幅+ダブル転換",
            "- **A**: 有力候補。複数の好条件が揃う",
            "- **B**: 検討候補。基本条件を満たすが追加確認推奨",
            "- **C**: 要精査。条件が最低限のみ",
            "",
        ])

    if df.empty:
        lines.append("該当銘柄なし")
    else:
        # 推奨度でソート（S > A > B > C）
        if has_rec:
            grade_order = {"S": 0, "A": 1, "B": 2, "C": 3}
            df = df.copy()
            df["_grade_order"] = df["Recommendation"].map(grade_order)
            df = df.sort_values("_grade_order").drop(columns=["_grade_order"])

        # ヘッダ
        header = "| 推奨 | コード | 銘柄名 | 株価(円) | 時価総額(億円) | 営業利益(億円) | 経常利益(億円) | 前期営業(億円) | 前期経常(億円) |"
        sep    = "|------|--------|--------|----------|---------------|---------------|---------------|----------------|----------------|"
        if not has_rec:
            header = "| コード | 銘柄名 | 株価(円) | 時価総額(億円) | 営業利益(億円) | 経常利益(億円) | 前期営業(億円) | 前期経常(億円) |"
            sep    = "|--------|--------|----------|---------------|---------------|---------------|----------------|----------------|"
        if has_fake:
            header += " 注意フラグ |"
            sep += "------------|"
        if has_rec:
            header += " 推奨理由 |"
            sep += "------------|"
        lines.append(header)
        lines.append(sep)

        for _, row in df.iterrows():
            code = row.get("Code", "")
            name = row.get("CompanyName", row.get("Name", ""))
            close = row.get("Close", 0)
            mcap = row.get("MarketCapitalization", None)
            if mcap is not None and pd.notna(mcap) and mcap > 0:
                mcap_oku = f"{mcap / 1e8:.1f}"
            else:
                mcap_oku = "不明"
            op = row.get("OperatingProfit", 0) or 0
            ordp = row.get("OrdinaryProfit", 0) or 0
            prev_op = row.get("prev_operating_profit", 0) or 0
            prev_ordp = row.get("prev_ordinary_profit", 0) or 0

            if has_rec:
                rec = row.get("Recommendation", "")
                line = f"| **{rec}** | {code} | {name} | {close:,.0f} | {mcap_oku} | {op:,.0f} | {ordp:,.0f} | {prev_op:,.0f} | {prev_ordp:,.0f} |"
            else:
                line = f"| {code} | {name} | {close:,.0f} | {mcap_oku} | {op:,.0f} | {ordp:,.0f} | {prev_op:,.0f} | {prev_ordp:,.0f} |"
            if has_fake:
                flags = row.get("fake_flags", "なし")
                line += f" {flags} |"
            if has_rec:
                reasons = row.get("RecReasons", "")
                line += f" {reasons} |"
            lines.append(line)

    # 個別銘柄リンク集
    if not df.empty:
        lines.extend(["", "## 調査リンク", ""])
        for _, row in df.iterrows():
            code = row.get("Code", "")
            name = row.get("CompanyName", row.get("Name", ""))
            rec = f"[{row.get('Recommendation', '-')}] " if has_rec else ""
            lines.append(
                f"- {rec}**{code} {name}**: "
                f"[IR Bank](https://irbank.net/{code}) | "
                f"[銘柄スカウター](https://monex.ifis.co.jp/index.php?sa=report_zaimu&bcode={code}) | "
                f"[Yahoo](https://finance.yahoo.co.jp/quote/{code}.T)"
            )

    lines.extend([
        "",
        "---",
        "",
        "> **注意:** 投資判断は必ず人間がレビューしてください。",
        "> 上記リンクから各銘柄の詳細を確認してください。",
        "> フェイク銘柄排除チェック（決算短信の特別損益・一過性要因）を確認してください。",
    ])

    content = "\n".join(lines) + "\n"
    output_path.write_text(content, encoding="utf-8")
    return str(output_path)
