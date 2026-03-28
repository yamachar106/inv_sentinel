# 黒字転換2倍株スクリーニング自動化プロジェクト

## プロジェクト概要
馬渕磨理子「黒字転換2倍株で勝つ投資術」のロジックを実装した日本株スクリーニング自動化システム。

**戦略の核心：**
- 四半期ベースで営業利益・経常利益が赤字→黒字に転換した銘柄を抽出
- フェイク銘柄（一過性黒字）を自動フィルタで除外
- 時価総額500億円以下・株価500〜2,500円の中小型株が対象
- 20〜30銘柄のウォッチリストを四半期ごとに生成
- 結果をSlackに通知

**売却ルール（書籍準拠 + 改良版）：**
- 株価2倍達成 → 利確
- 翌四半期で再び赤字転落 → 利益・損失問わず即売却
- トレーリングストップ（+80%到達後、高値-20%で利確）
- 損切りライン -30%
- 最大保有期間 2年

**購入推奨度（S/A/B/C）：**
- S: 最有力候補（長期赤字復活+大転換幅+ダブル転換）
- A: 有力候補（複数好条件）
- B: 検討候補（追加確認推奨）
- C: 要精査（基本見送り）
- 評価軸: 連続赤字期間(最重要)、転換幅、ダブル転換、黒字厚さ、フェイクスコア、時価総額

**エントリーフィルタ（核心）：**
- 連続2Q以上の赤字→黒字転換のみシグナル（振り子・季節パターン除外）

**売却ルール（書籍準拠 + 改良版）：**
- 株価2倍達成 → 利確
- 2Q連続赤字転落 → 利益・損失問わず即売却（1Qの一時的悪化では売らない）
- トレーリングストップ（+80%到達後、高値-20%で利確）
- 損切りライン -30%
- 最大保有期間 2年

---

## データソース構成（コストゼロ）

| ソース | 取得データ | 用途 |
|--------|-----------|------|
| **IR Bank** (無料) | Q1〜Q4の単独四半期 営業利益・経常利益・通期予想・進捗率 | 黒字転換スクリーニング + フェイクフィルタ |
| **yfinance** (無料) | 株価・時価総額 | 株価500〜2,500円・時価総額500億円以下フィルタ |
| **EDINET API** (無料) | H1累計(半期報告書)・FY(有報)のXBRL | クロスチェック用（`--edinet`フラグ） |

---

## ディレクトリ構成

```
kuroten-screener/
├── CLAUDE.md              ← このファイル
├── .env.example           ← APIキー設定例
├── requirements.txt       ← Python依存パッケージ
├── main.py                ← メインスクリプト
├── backtest.py            ← バックテスト
├── screener/
│   ├── config.py          ← 全パラメータ一元管理
│   ├── logger.py          ← ログ設定（コンソール+ファイル）
│   ├── irbank.py          ← IR Bankスクレイピング（リトライ付き）
│   ├── yfinance_client.py ← yfinance株価・時価総額取得
│   ├── edinet.py          ← EDINET API v2クライアント（オプション）
│   ├── fake_filter.py     ← フェイク銘柄フィルタ
│   ├── recommendation.py  ← 購入推奨度スコアリング（S/A/B/C、相対評価）
│   ├── filters.py         ← 株価・時価総額フィルタ
│   ├── notifier.py        ← Slack通知
│   └── reporter.py        ← ウォッチリスト生成（リンク付き）
├── data/
│   ├── watchlist/         ← 生成されたウォッチリスト（YYYY-QN.md）
│   ├── backtest/          ← バックテスト結果CSV
│   ├── logs/              ← 実行ログ（日付別）
│   └── cache/             ← APIレスポンスキャッシュ
└── tests/
    ├── test_filters.py
    ├── test_irbank.py
    ├── test_edinet.py
    ├── test_fake_filter.py
    ├── test_recommendation.py
    └── test_integration.py
```

---

## 環境設定

### APIキー（.envに記載）
```
EDINET_API_KEY=xxxx            # EDINET APIキー（--edinetフラグ使用時のみ必要）
SLACK_WEBHOOK_URL=xxxx         # Slack Incoming Webhook URL
```

### インストール
```bash
pip install -r requirements.txt
```

### 使い方
```bash
# スクリーニング（全銘柄: 約4時間、2.5秒/社）
python main.py                     # 本日日付でスクリーニング実行+Slack通知
python main.py --no-notify         # Slack通知をスキップ
python main.py --limit 200         # テスト実行（先頭200社のみ、約10分）
python main.py --date 20260315     # 指定日付で実行
python main.py --edinet            # EDINETデータでクロスチェック
python main.py --no-fake-filter    # フェイクフィルタをスキップ

# バックテスト
python backtest.py --codes 3656,2158,6758 --verbose  # 指定銘柄
python backtest.py --sample 20                       # ランダム20銘柄
python backtest.py --sample 30 --with-fake-filter    # フェイクフィルタ付き
python backtest.py --sample 50 --min-red 3           # 連続3Q以上赤字のみ
python backtest.py --sample 100 --seed 2026          # 再現性のあるランダム
python backtest.py --all                             # 全銘柄バックテスト
python backtest.py --sample 50 --grade-filter A      # 推奨度A以上のみ
python backtest.py --sample 50 --book-filter         # 書籍条件(500-2500円)
```

---

## 処理フロー

```
IR Bank (全銘柄四半期データ)
  ↓ スクレイピング（キャッシュあり）
黒字転換判定（連続2Q以上赤字→黒字転換）
  ↓ 候補: ~50-100社
フェイク銘柄フィルタ
  ├ 通期予想が赤字 → 除外
  ├ Q4偏重パターン（Q1-Q3赤字→Q4のみ黒字が繰返し） → 除外
  ├ 直近3年中2年以上が通期赤字 → 除外
  └ 繰り返し黒字転換3回以上（ココナラ型） → 除外
  ↓ 候補: ~50-100社
yfinance（候補銘柄の株価・時価総額）
  ↓ バッチ取得
株価500-2500円 / 時価総額500億円以下フィルタ
  ↓ ~20-30社
購入推奨度スコアリング（S/A/B/C）
  ↓
ウォッチリスト生成（Markdown + 推奨度・理由付き）
  ↓
Slack通知
```

---

## フェイク銘柄フィルタ

書籍の「フェイク銘柄排除」ロジックを自動化。スコアリング方式で判定（score >= 2 で除外）。

| チェック項目 | スコア | 説明 |
|-------------|--------|------|
| 通期営業利益予想が赤字 | +2 | 最重要。これだけで除外 |
| 通期予想データなし | +1 | 予想未開示企業 |
| 進捗率が例年レンジの70%未満 | +1 | 利益の健全性に疑問 |
| Q4偏重パターン(2回) | +1 | 年末の一過性調整 |
| Q4偏重パターン(3回以上) | +2 | 構造的な問題 |
| 通期赤字歴(直近3年中2年以上) | +1 | 赤字体質 |
| 繰り返し黒字転換(3回以上) | +1 | 黒字が定着しないココナラ型 |

---

## スクリーニング条件（設定可能）

| パラメータ | デフォルト値 | 変数名 | ファイル |
|-----------|------------|--------|---------|
| 時価総額上限 | 500億円 | `MAX_MARKET_CAP` | filters.py |
| 株価下限 | 500円 | `MIN_PRICE` | filters.py |
| 株価上限 | 2,500円 | `MAX_PRICE` | filters.py |
| 連続赤字Q数(最低) | 2 | `MIN_CONSECUTIVE_RED` | backtest.py / irbank.py |
| フェイクスコア閾値 | 2 | score >= 2 | fake_filter.py |
| 損切りライン | -30% | `STOP_LOSS_PCT` | backtest.py |
| 利確目標 | 2倍 | `SELL_TARGET` | backtest.py |
| 最大保有期間 | 2年 | `MAX_HOLD_YEARS` | backtest.py |
| トレーリング発動 | +80% | `TRAILING_STOP_TRIGGER` | backtest.py |
| トレーリング幅 | 高値-20% | `TRAILING_STOP_PCT` | backtest.py |
| 投資単位 | 100万円/trade | `PER_TRADE_CAPITAL` | backtest.py |

---

## 注意事項

### データソース
- IR Bank: スクレイピングのためリクエスト間隔1.5秒を遵守。値は億円単位
- yfinance: 非公式APIのため仕様変更の可能性あり。小型株の時価総額がNull多い
- EDINET: IFRS・米国会計基準の企業は経常利益フィールドが空欄になる

### 投資判断
- **スクリーニング結果は必ず人間がレビューすること**
- フェイクフィルタはscore=1（要注意）の銘柄も出力するので手動確認推奨
- 発注前にマネックス銘柄スカウターでクロスチェック推奨

---

## 参考資料
- 調査レポート：`C:\MyUniverse\SecondBrain\03_Resources\references\kuroten2bai_research.md`
- EDINET API: https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0110.html
