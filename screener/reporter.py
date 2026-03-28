"""
ウォッチリスト生成
スクリーニング結果をMarkdownファイルとして出力する
"""

import re
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "watchlist"


def _quarter_label(date_str: str) -> str:
    """日付文字列 (YYYYMMDD) から四半期ラベルを返す (例: 2026-Q1)"""
    dt = datetime.strptime(date_str, "%Y%m%d")
    quarter = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{quarter}"


def load_previous_watchlist(current_date: str) -> set[str]:
    """
    現在日付より前の最新ウォッチリスト.mdから銘柄コードを抽出する

    Args:
        current_date: 対象日付 (YYYYMMDD)

    Returns:
        前回ウォッチリストの銘柄コードのset
    """
    current_label = _quarter_label(current_date)

    # data/watchlist/ 内の .md ファイルを列挙し、現在ラベルより前のものを探す
    if not DATA_DIR.exists():
        return set()

    candidates = []
    for p in DATA_DIR.glob("*.md"):
        label = p.stem  # e.g. "2026-Q1"
        if label < current_label:
            candidates.append(p)

    if not candidates:
        return set()

    # ファイル名(ラベル)で降順ソートし、最新を取得
    candidates.sort(key=lambda p: p.stem, reverse=True)
    prev_path = candidates[0]

    # Markdownテーブルから銘柄コードを抽出
    codes: set[str] = set()
    content = prev_path.read_text(encoding="utf-8")
    for line in content.split("\n"):
        if not line.startswith("|"):
            continue
        # ヘッダ行・セパレータ行をスキップ
        if "---" in line or "コード" in line:
            continue
        # テーブル行からコードを抽出 (4桁数字)
        cells = [c.strip() for c in line.split("|")]
        for cell in cells:
            # "**A**" などの推奨度セルを飛ばし、4桁の数字セルを探す
            clean = cell.strip("* ")
            if re.fullmatch(r"\d{4}", clean):
                codes.add(clean)
                break
    return codes


def compute_diff(
    current_codes: set[str], previous_codes: set[str]
) -> tuple[set[str], set[str]]:
    """
    現在と前回のウォッチリストの差分を計算する

    Returns:
        (new_additions, removals)
    """
    new_additions = current_codes - previous_codes
    removals = previous_codes - current_codes
    return new_additions, removals


def _format_trend(values: list[float], unit: str = "億") -> str:
    """数値リストを「10.5億 → 11.2億 → ...」形式の文字列に変換する"""
    if not values:
        return "データなし"
    parts = []
    for v in values:
        if v is None:
            parts.append("-")
        else:
            parts.append(f"{v:,.1f}{unit}")
    return " → ".join(parts)


def generate_watchlist(
    df: pd.DataFrame, date: str, company_summaries: dict[str, dict] | None = None,
) -> tuple[str, set[str], set[str]]:
    """
    ウォッチリストをMarkdownファイルとして生成する

    Args:
        df: フィルタ済みのDataFrame
        date: 対象日付 (YYYYMMDD)

    Returns:
        (出力ファイルパス, 新規追加コードset, 脱落コードset)
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    label = _quarter_label(date)
    output_path = DATA_DIR / f"{label}.md"

    # 前回ウォッチリストとの差分を計算
    current_codes = set(df["Code"].astype(str).tolist()) if not df.empty else set()
    previous_codes = load_previous_watchlist(date)
    new_additions, removals = compute_diff(current_codes, previous_codes)

    # コード→銘柄名マッピング (現在のdf内)
    code_to_name: dict[str, str] = {}
    if not df.empty:
        for _, row in df.iterrows():
            code_to_name[str(row.get("Code", ""))] = row.get(
                "CompanyName", row.get("Name", "")
            )

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

    # 銘柄詳細セクション
    if not df.empty and company_summaries:
        lines.extend(["", "## 銘柄詳細", ""])
        for _, row in df.iterrows():
            code = str(row.get("Code", ""))
            name = row.get("CompanyName", row.get("Name", ""))
            rec = f" [{row.get('Recommendation', '-')}]" if has_rec else ""
            summary = company_summaries.get(code)
            if summary is None:
                continue

            lines.append(f"### {code} {name}{rec}")

            rev_trend = summary.get("revenue_trend", [])
            op_trend = summary.get("op_trend", [])
            yoy_revenue = summary.get("yoy_revenue")
            yoy_op = summary.get("yoy_op")

            if rev_trend:
                lines.append(f"- 売上推移(直近{len(rev_trend)}Q): {_format_trend(rev_trend)}")
            if op_trend:
                # 黒字転換を強調表示
                op_str = _format_trend(op_trend)
                if op_trend and op_trend[-1] is not None and op_trend[-1] > 0:
                    # 直前が赤字なら黒字転換マークを付ける
                    if len(op_trend) >= 2 and op_trend[-2] is not None and op_trend[-2] < 0:
                        op_str += " (黒字転換!)"
                lines.append(f"- 営業利益推移: {op_str}")
            if yoy_revenue:
                lines.append(f"- 前年同期比売上: {yoy_revenue}")
            if yoy_op:
                lines.append(f"- 前年同期比営業利益: {yoy_op}")
            lines.append("")

    # 変動セクション (前回との差分)
    if previous_codes:
        lines.extend(["", "## 変動", ""])
        if new_additions:
            names = [
                f"{c} {code_to_name.get(c, '')}" for c in sorted(new_additions)
            ]
            lines.append(f"**新規追加:** {', '.join(names)}")
        else:
            lines.append("**新規追加:** なし")
        if removals:
            names = [f"{c}" for c in sorted(removals)]
            lines.append(f"**脱落:** {', '.join(names)}")
        else:
            lines.append("**脱落:** なし")

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
    return str(output_path), new_additions, removals
