# 黒字転換2倍株スクリーニング

馬渕磨理子「黒字転換2倍株で勝つ投資術」のロジックを自動化した日本株スクリーニングシステム。

## 概要

四半期ベースで営業利益・経常利益が**赤字→黒字に転換**した中小型株を自動抽出し、フェイク銘柄を除外した上でウォッチリストを生成します。

**特徴:**
- コストゼロ（IR Bank + yfinance + EDINET API）
- フェイク銘柄自動フィルタ（通期予想赤字、Q4偏重、繰り返し転換を検出）
- 購入推奨度スコアリング（S/A/B/C）
- バックテスト付き（チャート出力対応）
- GitHub Actionsで四半期自動実行
- Slack通知（前回との差分付き）

## クイックスタート

```bash
# セットアップ
pip install -r requirements.txt
cp .env.example .env  # 必要に応じてAPIキーを設定

# テスト実行（先頭200社、約10分）
python main.py --limit 200 --no-notify

# 全銘柄スクリーニング（約4時間、2回目以降はキャッシュで高速）
python main.py --no-notify

# バックテスト
python backtest.py --sample 30 --verbose
```

## スクリーニング条件

| 条件 | 値 |
|------|-----|
| 株価 | 500〜2,500円 |
| 時価総額 | 500億円以下 |
| 黒字転換 | 連続2Q以上赤字→黒字 |
| フェイク除外 | スコア2以上を除外 |

## 出力例

スクリーニング結果は `data/watchlist/` にMarkdownで出力されます:

```
| 推奨 | コード | 銘柄名 | 株価(円) | 時価総額(億円) | ...
|------|--------|--------|----------|---------------|
| **S** | 1234 | テスト株式会社 | 1,500 | 150.0 | ...
```

## GitHub Actions

| ワークフロー | トリガー | 用途 |
|-------------|---------|------|
| Quarterly Screening | 2/5/8/11月15日 + 手動 | 全銘柄スクリーニング |
| Tests | push/PR | テスト自動実行 |

### セットアップ

1. リポジトリのSettings > Secrets and variables > Actionsで設定:
   - `SLACK_WEBHOOK_URL` — Slack通知用（任意）
   - `EDINET_API_KEY` — EDINETクロスチェック用（任意）
2. Actions > Quarterly Screening > Run workflow で手動実行可能

## プロジェクト構成

```
├── main.py               # メインスクリプト
├── backtest.py            # バックテスト
├── screener/
│   ├── config.py          # 全パラメータ一元管理
│   ├── irbank.py          # IR Bankスクレイピング（キャッシュ付き）
│   ├── yfinance_client.py # 株価・時価総額取得
│   ├── fake_filter.py     # フェイク銘柄フィルタ
│   ├── recommendation.py  # 推奨度スコアリング（S/A/B/C）
│   ├── filters.py         # 株価・時価総額フィルタ
│   ├── reporter.py        # ウォッチリスト生成（差分付き）
│   ├── visualizer.py      # バックテストチャート出力
│   ├── notifier.py        # Slack通知
│   ├── edinet.py          # EDINET APIクライアント
│   └── logger.py          # ログ設定
├── tests/                 # テスト（103件）
├── data/
│   ├── watchlist/         # スクリーニング結果（Git管理）
│   └── backtest/          # バックテスト結果（Git管理）
└── .github/workflows/     # GitHub Actions
```

## 注意事項

- **投資判断は必ず人間がレビューしてください**
- マネックス銘柄スカウターでクロスチェック推奨
- IR Bankへのリクエスト間隔は2.5秒を遵守しています
