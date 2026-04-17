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
- 株価2倍達成 → 利確（書籍準拠）
- 2Q連続赤字転落 → 利益・損失問わず即売却（1Qの一時的悪化では売らない）
- 利益成長の鈍化 → 手放す（書籍核心ルール）
- トレーリングストップ（+80%到達後、高値-20%で利確）
- 損切りライン -10%（書籍コンセンサス: DUKE(ガチ)-10%, オニール-8%, kenmo-8%）
- 最大保有期間 2年

**購入推奨度（S/A/B/C）：**
- S: 最有力候補（長期赤字復活+大転換幅+ダブル転換）
- A: 有力候補（複数好条件）
- B: 検討候補（追加確認推奨）
- C: 要精査（基本見送り）
- 評価軸: 連続赤字期間(最重要)、転換幅、ダブル転換、黒字厚さ、フェイクスコア、時価総額

**エントリーフィルタ（核心）：**
- 連続2Q以上の赤字→黒字転換のみシグナル（振り子・季節パターン除外）

---

## データソース構成（コストゼロ）

| ソース | 取得データ | 用途 |
|--------|-----------|------|
| **IR Bank** (無料) | Q1〜Q4の単独四半期 営業利益・経常利益・通期予想・進捗率 | 黒字転換スクリーニング + フェイクフィルタ |
| **yfinance** (無料) | 株価・時価総額 | 株価500〜2,500円・時価総額500億円以下フィルタ |
| **NASDAQ Screener API** (無料) | 全米株7,000+銘柄のシンボル・時価総額・セクター | 米国株ユニバース管理 |
| **EDINET API** (無料) | H1累計(半期報告書)・FY(有報)のXBRL | クロスチェック用（`--edinet`フラグ） |
| **Gemini Flash API** (無料) | AI分析・企業情報翻訳 | ダッシュボードAI分析 (1,000 RPD無料枠) |

---

## ダッシュボード

- **本番URL**: https://mega-breakout.streamlit.app
- **ホスティング**: Streamlit Community Cloud（GitHub連携、自動デプロイ）
- **ローカル起動**: `streamlit run dashboard/app.py`
- **キャッシュ更新**: `python dashboard/refresh_cache.py`（daily_run.py後に実行推奨）
- **Secrets**: Streamlit Cloud の Settings → Secrets に `GOOGLE_API_KEY` を設定

---

## ディレクトリ構成

```
kuroten-screener/
├── CLAUDE.md              ← このファイル
├── .env.example           ← APIキー設定例
├── requirements.txt       ← Python依存パッケージ
├── main.py                ← メインスクリプト（黒字転換スクリーニング）
├── backtest.py            ← 黒字転換バックテスト
├── breakout_monitor.py    ← 新高値ブレイクアウト監視CLI
├── backtest_breakout.py   ← ブレイクアウト戦略バックテスト
├── daily_run.py           ← 統合デイリーランナー（全戦略一括実行）
├── screener/
│   ├── config.py          ← 全パラメータ一元管理
│   ├── logger.py          ← ログ設定（コンソール+ファイル）
│   ├── irbank.py          ← IR Bankスクレイピング（リトライ付き）
│   ├── yfinance_client.py ← yfinance株価・時価総額取得
│   ├── edinet.py          ← EDINET API v2クライアント（オプション）
│   ├── fake_filter.py     ← フェイク銘柄フィルタ
│   ├── recommendation.py  ← 購入推奨度スコアリング（S/A/B/C、相対評価）
│   ├── breakout.py        ← 新高値ブレイクアウト検出（JP/US対応、バッチ取得）
│   ├── earnings.py        ← Earnings Acceleration検出（CAN SLIM C+A、ブレイクアウト確度フィルタ）
│   ├── universe.py        ← 銘柄ユニバース管理（NASDAQ API、7000+銘柄）
│   ├── exclusion.py       ← 除外フィルタ（REIT/ETF/優先株等、キャッシュ付き）
│   ├── signal_store.py    ← シグナル履歴永続化（初出/継続/消失判定）
│   ├── healthcheck.py     ← データソース疎通確認
│   ├── filters.py         ← 株価・時価総額フィルタ
│   ├── notifier.py        ← Slack通知（strategy×marketルーティング+売却シグナル対応）
│   ├── portfolio.py       ← ポジション管理CRUD+CLI
│   ├── sell_monitor.py    ← 売却シグナル監視（5ルール）
│   ├── market_regime.py   ← 相場環境判定（BULL/NEUTRAL/BEAR）
│   ├── performance.py     ← トレード履歴・勝率・PF統計
│   ├── rs_ranking.py      ← Relative Strength ランキング（ブレイクアウト前段フィルタ）
│   └── reporter.py        ← ウォッチリスト生成（リンク付き）
├── data/
│   ├── portfolio.json     ← 保有ポジション管理
│   ├── portfolio_history.json ← クローズ済みトレード履歴
│   ├── watchlist/         ← 生成されたウォッチリスト（YYYY-QN.md）
│   ├── backtest/          ← バックテスト結果CSV
│   ├── signals/           ← 日次シグナル履歴（YYYY-MM-DD.json）
│   ├── universe/          ← 銘柄ユニバースキャッシュ
│   ├── references/        ← 参考書籍・資料データ
│   ├── logs/              ← 実行ログ（日付別）
│   └── cache/             ← APIレスポンスキャッシュ（除外コード含む）
└── tests/
    ├── test_breakout.py
    ├── test_filters.py
    ├── test_irbank.py
    ├── test_edinet.py
    ├── test_fake_filter.py
    ├── test_recommendation.py
    ├── test_notifier.py
    ├── test_universe.py
    ├── test_exclusion.py
    ├── test_signal_store.py
    ├── test_healthcheck.py
    ├── test_earnings.py
    ├── test_portfolio.py
    ├── test_sell_monitor.py
    ├── test_market_regime.py
    ├── test_performance.py
    ├── test_rs_ranking.py
    └── test_integration.py
```

---

## 環境設定

### APIキー（.envに記載）
```
EDINET_API_KEY=xxxx            # EDINET APIキー（--edinetフラグ使用時のみ必要）
SLACK_WEBHOOK_URL=xxxx         # Slack Incoming Webhook URL（フォールバック）
SLACK_WEBHOOK_KUROTEN_JP=xxxx  # 黒字転換JP専用チャンネル（任意）
SLACK_WEBHOOK_BREAKOUT_JP=xxxx # ブレイクアウトJP専用チャンネル（任意）
SLACK_WEBHOOK_BREAKOUT_US=xxxx # ブレイクアウトUS専用チャンネル（任意）
SLACK_WEBHOOK_MEGA_US=xxxx    # Mega ($200B+) 専用チャンネル（任意）
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

# ブレイクアウト監視
python breakout_monitor.py                                  # JP: 最新ウォッチリスト全銘柄
python breakout_monitor.py --codes 7974,6758                # JP: 指定銘柄
python breakout_monitor.py --market US --universe us_all    # US: 全米株（$300M-$50B）
python breakout_monitor.py --market US --universe us_mid    # US: 中型株（$2B-$10B）
python breakout_monitor.py --market US --universe us_small  # US: 小型株（$300M-$2B）
python breakout_monitor.py --market US --universe sp500     # US: S&P500相当（$14B+）
python breakout_monitor.py --market US --codes AAPL,MSFT    # US: 指定銘柄
python breakout_monitor.py --market US --universe us_mid --limit 50  # テスト実行
python breakout_monitor.py --no-notify                      # Slack通知スキップ

# 統合デイリーランナー
python daily_run.py                                # 全戦略実行（JP全区分+US）
python daily_run.py --strategy breakout            # ブレイクアウトのみ
python daily_run.py --market US                    # US市場のみ
python daily_run.py --strategy monitor             # ポジション監視のみ（売却シグナル検出）
python daily_run.py --dry-run                      # 通知なしの実行プレビュー
python daily_run.py --universe us_mid --limit 100  # USユニバース指定
python daily_run.py --jp-universe jp_growth        # JP Growthのみ（旧デフォルト）

# ブレイクアウト バックテスト
python backtest_breakout.py --codes AAPL,MSFT,NVDA         # 指定銘柄
python backtest_breakout.py --codes 7974,6758 --market JP  # 日本株
python backtest_breakout.py --universe us_mid --limit 50   # USユニバース

# ポジション管理
python -m screener.portfolio add 3656 --strategy kuroten --buy-date 2026-03-15 --buy-price 1250 --shares 800
python -m screener.portfolio add 3656 --strategy kuroten --buy-date 2026-03-15 --buy-price 1250 --shares 800 --notes "S評価"
python -m screener.portfolio list                          # 全ポジション
python -m screener.portfolio list --strategy kuroten       # 戦略フィルタ
python -m screener.portfolio remove 3656                   # 単純削除
python -m screener.portfolio remove 3656 --sell-price 2500 --sell-reason "2倍達成"  # 売却記録付き
```

---

## 処理フロー

```
IR Bank (全銘柄四半期データ)
  ↓ スクレイピング（キャッシュあり）
黒字転換判定（連続2Q以上赤字→黒字転換）
  ↓ 候補: ~50-100社
フェイク銘柄フィルタ
  ├ 業種フィルタ（医薬品・バイオ・創薬・ゲーム関連） → 除外
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

--- daily_run.py 統合フロー ---

[0] ヘルスチェック
[!] キャッシュ無効化（本決算シーズン）
[1] JP ブレイクアウト監視
[2] JP 黒字転換 日次チェック
[3] JP 上場市場変更監視
[4] US ブレイクアウト監視
[5] GCペンディングチェック
[6] ポジション監視（売却シグナル5ルール → Slack通知、エラー時も通知）
[7] 相場環境判定（SMA50/SMA200 → BULL/NEUTRAL/BEAR）
[8] シグナル保存 + 差分計算
[9] ダイジェスト通知（regime header + 売却シグナル件数）
```

---

## フェイク銘柄フィルタ

書籍の「フェイク銘柄排除」ロジックを自動化。スコアリング方式で判定（score >= 2 で除外）。

| チェック項目 | スコア | 説明 |
|-------------|--------|------|
| 除外業種(医薬品) / 除外キーワード(バイオ,創薬,ゲーム) | +2 | 書籍第2章: 不確定要素多く除外推奨 |
| 通期営業利益予想が赤字 | +2 | 最重要。これだけで除外 |
| 通期予想データなし | +1 | 予想未開示企業 |
| 進捗率が例年レンジの70%未満 | +1 | 利益の健全性に疑問 |
| Q4偏重パターン(2回) | +1 | 年末の一過性調整 |
| Q4偏重パターン(3回以上) | +2 | 構造的な問題 |
| 通期赤字歴(直近3年中2年以上) | +1 | 赤字体質 |
| 繰り返し黒字転換(3回以上) | +1 | 黒字が定着しないココナラ型 |

---

## スクリーニング条件（設定可能）

### 黒字転換（config.py）

| パラメータ | デフォルト値 | 変数名 | 根拠 |
|-----------|------------|--------|------|
| 時価総額上限 | 500億円 | `MAX_MARKET_CAP` | 馬渕本 |
| 株価下限 | 500円 | `MIN_PRICE` | 馬渕本 |
| 株価上限 | 2,500円 | `MAX_PRICE` | 馬渕本 |
| 連続赤字Q数(最低) | 2 | `MIN_CONSECUTIVE_RED` | 馬渕本+季節パターン除外 |
| 損切りライン | -10% | `STOP_LOSS_PCT` | DUKE(ガチ)-10%準拠 |
| 利確目標 | 2倍 | `SELL_TARGET` | 馬渕本 |
| 最大保有期間 | 2年 | `MAX_HOLD_YEARS` | 馬渕本 |
| トレーリング発動 | +80% | `TRAILING_STOP_TRIGGER` | 独自改良 |
| トレーリング幅 | 高値-20% | `TRAILING_STOP_PCT` | 独自改良 |

### ブレイクアウト（config.py）

| パラメータ | デフォルト値 | 変数名 | 根拠 |
|-----------|------------|--------|------|
| 52W高値ルックバック | 252日 | `BREAKOUT_52W_WINDOW` | 標準 |
| SMA200必須 | True | `BREAKOUT_REQUIRE_ABOVE_SMA200` | Minervini/DUKE/Ryan全書籍 |
| 出来高比率(JP) | 1.5倍 | `BREAKOUT_VOLUME_RATIO` | オニール+40〜50%=1.5倍 |
| 出来高比率(US) | 3.0倍 | `BREAKOUT_VOLUME_RATIO_US` | バックテスト検証(勝率57%) |
| RSI過熱閾値 | 75 | `BREAKOUT_PULLBACK_RSI_MAX` | 独自（バックテスト検証） |
| 損切り | -10% | `BREAKOUT_STOP_LOSS` | DUKE(ガチ)/オニール-8%/kenmo-8% |
| 標準利確 | +20% | `BREAKOUT_PROFIT_TARGET` | オニール/DUKE(ガチ) |
| 延長利確 | +25% | `BREAKOUT_PROFIT_TARGET_EXTENDED` | オニール8週ルール |
| 時価総額(JP優先) | 200億 | `BREAKOUT_MAX_MARKET_CAP_JP` | DUKE:10倍株92.4%が200億未満 |
| 時価総額(JP許容) | 500億 | `BREAKOUT_MAX_MARKET_CAP_JP_LOOSE` | 拡大検索用 |
| 有望セクター | IT/サービス/小売 | `BREAKOUT_PREFERRED_SECTORS_JP` | DUKE統計 |

### Earnings Acceleration（config.py）

| パラメータ | デフォルト値 | 変数名 | 根拠 |
|-----------|------------|--------|------|
| 最低利益成長率 | +25% | `EA_MIN_PROFIT_GROWTH` | O'Neill CAN SLIM "C" |
| 加速判定閾値 | 0% | `EA_MIN_ACCELERATION` | 前期比改善ならOK |
| 連続加速四半期数 | 2 | `EA_MIN_CONSECUTIVE` | O'Neill: 2Q以上必須 |
| 最低売上成長率 | +10% | `EA_MIN_REVENUE_GROWTH` | O'Neill: 売上伴わない利益成長は除外 |
| 売上バリデーション | 有効 | `EA_REQUIRE_REVENUE_VALIDATION` | O'Neill準拠 |
| US向け連続加速緩和 | 1Q | `min_consecutive_override=1` | yfinance制約(~5Q→YoY1回) |

### 売却監視（config.py）

| パラメータ | デフォルト値 | 変数名 | 根拠 |
|-----------|------------|--------|------|
| 赤字チェック間隔 | 7日 | `SELL_MONITOR_DEFICIT_CHECK_INTERVAL` | 週1回で十分 |
| BEAR時買い抑制 | False | `MARKET_REGIME_SUPPRESS_IN_BEAR` | オプション |

売却ルール自体は黒字転換（`SELL_TARGET`, `STOP_LOSS_PCT`等）とブレイクアウト（`BREAKOUT_PROFIT_TARGET`, `BREAKOUT_STOP_LOSS`等）の既存パラメータを共有。

### Relative Strength ランキング（config.py）

| パラメータ | デフォルト値 | 変数名 | 根拠 |
|-----------|------------|--------|------|
| ルックバック期間 | 126日 | `RS_LOOKBACK_DAYS` | 約6ヶ月（O'Neill準拠） |
| JP最低パーセンタイル | 70 | `RS_MIN_PERCENTILE_JP` | 上位30% |
| US最低パーセンタイル | 70 | `RS_MIN_PERCENTILE_US` | 上位30% |
| RS有効化 | True | `RS_ENABLED` | ブレイクアウト前段フィルタ |

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
- 書籍統合ルール対照表：`data/references/synthesis_all_books.md`（全9冊の投資ルール比較）
- 書籍データ（9冊）：`data/references/`
- 調査レポート：`C:\MyUniverse\SecondBrain\03_Resources\references\kuroten2bai_research.md`
- EDINET API: https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0110.html
