"""
黒字転換2倍株 共通設定パラメータ

スクリーニング・バックテスト・フィルタの全設定を一元管理する。
各モジュールはここからimportして使用すること。
"""

# =============================================================================
# システムキルスイッチ
# =============================================================================
# True にすると全ての外部通信（Slack通知、API呼び出し等）を無効化する。
# データは保持されるが、日次パイプラインは早期終了する。
SYSTEM_ENABLED = False

# =============================================================================
# スクリーニング条件（書籍準拠）
# =============================================================================

# 株価フィルタ（書籍準拠: 500-2500円）
MIN_PRICE = 500                     # 株価下限（円）
MAX_PRICE = 2_500                   # 株価上限（円）
MAX_MARKET_CAP = 50_000_000_000     # 時価総額上限（500億円）

# 連続赤字四半期数（ノイズ除去の核心パラメータ）
MIN_CONSECUTIVE_RED = 2             # 最低連続赤字四半期数

# =============================================================================
# バックテスト設定
# =============================================================================

# 売買ルール
SELL_TARGET = 2.0                   # 利確目標（2倍）
MAX_HOLD_YEARS = 2                  # 最大保有期間（年）
STOP_LOSS_PCT = -0.25               # 損切りライン（-25%）※BT90トレードsweep: -25%がPF4.16/勝率60%で最適（-20%比+23%改善）
TRAILING_STOP_TRIGGER = 0.80        # トレーリングストップ発動（+80%到達時）
TRAILING_STOP_PCT = -0.20           # 高値からの下落で利確（-20%）
PARTIAL_PROFIT_TARGET = 0.50        # 部分利確ライン（+50%で半分売却）
PARTIAL_PROFIT_RATIO = 0.50         # 部分利確時の売却割合（50%）
PER_TRADE_CAPITAL = 1_000_000       # 1トレードあたりの投資額（100万円）

# バックテスト用株価フィルタ（過去データ検証用に書籍より広め）
BT_MIN_PRICE = 100                  # バックテスト用 株価下限（円）
BT_MAX_PRICE = 5000                 # バックテスト用 株価上限（円）

# =============================================================================
# IR Bank スクレイピング設定
# =============================================================================

EDINET_CACHE_DAYS = 90              # EDINETキャッシュ有効期限（日）
IRBANK_CACHE_DAYS = 7               # IR Bank四半期データのキャッシュ有効期限（日）
IRBANK_CACHE_DAYS_EARNINGS = 1      # 本決算シーズン中のキャッシュ有効期限（日）

# 本決算シーズン（3月期決算: 4-5月、Q3短信: 1-2月）
EARNINGS_SEASON_MONTHS = [1, 2, 4, 5]

REQUEST_INTERVAL = 2.5              # リクエスト間隔（秒）
MAX_RETRIES = 3                     # HTTPリトライ回数
RETRY_BACKOFF = 5.0                 # リトライ間隔の基数（秒、指数増加）

# =============================================================================
# フェイクフィルタ設定
# =============================================================================

FAKE_SCORE_THRESHOLD = 2            # フェイク判定の閾値（この値以上でフェイク）
PROGRESS_RATIO_THRESHOLD = 0.7      # 進捗率の例年レンジに対する下限倍率

# 業種フィルタ（書籍推奨: バイオ・創薬・ゲーム関連は不確定要素多く除外）
RISKY_CATEGORIES = {"医薬品"}       # 東証業種分類で除外対象
RISKY_NAME_KEYWORDS = [             # 銘柄名に含まれるキーワードで除外対象
    "バイオ", "創薬", "ゲーム", "ゲームス",
]

# =============================================================================
# 推奨度スコアリング設定 v1（レガシー、後方互換用）
# =============================================================================

REC_RECOVERY_RATIO = 0.5            # 回復力判定: 当期黒字が前期赤字のこの倍率以上
REC_PROFIT_THICKNESS = 3            # 黒字の厚さ判定: 営業利益がこの値(億円)以上
REC_SMALL_CAP_THRESHOLD = 200       # 小型株判定: 時価総額がこの値(億円)以下
REC_GRADE_S = 8                     # Sランク: この点数以上
REC_GRADE_A = 5                     # Aランク: この点数以上
REC_GRADE_B = 3                     # Bランク: この点数以上

# =============================================================================
# 推奨度スコアリング設定 v2（厳選投資向け）
# =============================================================================

# --- 加点ファクター ---
# v2.1: バックテスト90トレードの因子分析に基づき再調整
# 旧YOY_SAME_Q_RED_BONUS=+2は逆効果（あり+7.1%/wr42% vs なし+9.7%/wr55%）→ 0に
REC_V2_YOY_SAME_Q_RED_BONUS = 0          # 前年同期も赤字 → 中立（v2.0: +2→v2.1: 0）
REC_V2_PROFIT_MCAP_HIGH = 0.02           # 利益/時価総額 > 2% → +2
REC_V2_PROFIT_MCAP_LOW = 0.005           # 利益/時価総額 > 0.5% → +1
# 旧REVENUE_GROWTH_HIGH=+2は逆効果（あり+2.7% vs なし+12.7%）→ +1/0に
REC_V2_REVENUE_GROWTH_HIGH = 0.15        # 前年同期比売上 > +15% → +1（v2.0: +2→v2.1: +1）
REC_V2_REVENUE_GROWTH_LOW = 0.05         # 前年同期比売上 > +5% → 0（v2.0: +1→v2.1: 0）
REC_V2_FORECAST_ALIGNED_BONUS = 1        # 通期予想黒字+進捗率健全 → +1

# --- 減点ファクター ---
# 旧SEASONAL_PENALTY=-2/-3は過剰（wr53%は平均以上）→ -1/-2に緩和
REC_V2_SEASONAL_PENALTY_MILD = -1        # 前年同期が黒字(1年)（v2.0: -2→v2.1: -1）
REC_V2_SEASONAL_PENALTY_STRONG = -2      # 前年同期が黒字(2年以上)（v2.0: -3→v2.1: -2）
REC_V2_PRIOR_FAILURE_PENALTY = -2        # 同銘柄の過去シグナル失敗
# 旧THIN_PROFIT=-2/-1は逆効果（薄利+22.1%/wr60% vs 非薄利+7.4%/wr50%）→ 0に
REC_V2_THIN_PROFIT_SEVERE = 0            # 営業利益 < 1億（v2.0: -2→v2.1: 0）
REC_V2_THIN_PROFIT_MILD = 0              # 営業利益 < 3億（v2.0: -1→v2.1: 0）
REC_V2_DEPTH_MISMATCH_PENALTY = -1       # 5Q+赤字で回復が弱い

# --- 重み削減（旧ファクター） ---
REC_V2_CONSECUTIVE_RED_BONUS = 1         # 連続赤字3Q以上: +1（旧: 最大+4）
REC_V2_DOUBLE_TURN_BONUS = 1             # ダブル転換: +1（旧: +2）

# --- F-Score連携 ---
REC_V2_FSCORE_HIGH = 5                   # F-Score >= 5 → +1（財務健全）
REC_V2_FSCORE_LOW = 2                    # F-Score <= 2 → -1（財務懸念）

# --- グレード閾値（v2.1: BT90トレード因子分析→単調性確認済み S>A>B） ---
REC_V2_GRADE_S = 3                       # S(Top Pick): BT avg+26%, n=4（v2.0: 5→v2.1: 3）
REC_V2_GRADE_A = 1                       # A(Strong): BT avg+11.4%, n=45（v2.0: 3→v2.1: 1）
REC_V2_GRADE_B = -1                      # B(Watch): BT avg+4.8%, n=41（v2.0: 1→v2.1: -1）

# =============================================================================
# ブレイクアウト監視設定
# =============================================================================

BREAKOUT_52W_WINDOW = 252            # ルックバック期間（営業日）
BREAKOUT_SMA_SHORT = 20
BREAKOUT_SMA_MID = 50
BREAKOUT_SMA_LONG = 200
BREAKOUT_VOLUME_RATIO = 1.5          # JP高出来高判定
BREAKOUT_PREBREAK_VOL = 1.2          # JPプレブレイクアウト出来高
BREAKOUT_NEAR_HIGH_UPPER = -2        # 近接上限(%)
BREAKOUT_NEAR_HIGH_LOWER = -5        # 近接下限(%) ※-8%→-5%に縮小（ノイズ削減）
BREAKOUT_HISTORY_PERIOD = "1y"
TICKER_SUFFIX_JP = ".T"
TICKER_SUFFIX_US = ""

# US固有の閾値（米国株は流動性が高いため出来高閾値を引き上げ）
BREAKOUT_VOLUME_RATIO_US = 3.0       # USブレイクアウト出来高 ※2.0→3.0（勝率57%帯に絞る）
BREAKOUT_PREBREAK_VOL_US = 1.5       # USプレブレイクアウト出来高

# BREAKOUTシグナルの押し目フィルタ（高値掴み回避）
BREAKOUT_PULLBACK_ENABLED = True     # 押し目フィルタを有効にするか
BREAKOUT_PULLBACK_RSI_MAX = 75       # RSIがこれ以上は過熱と見なし通知のみ

# SMA200フィルタ（全ブレイクアウト書籍で必須条件）
# Minervini「200日MA下の銘柄は絶対に買わない」/ DUKE「第2ステージのみ投資対象」
# BT検証: Prime +4.29%効果, Standard +3.51%効果, Growth -0.54%(微逆効果)
# BEAR期でもSMA200上ならEVプラス → 実質BEAR防御フィルタとして機能
BREAKOUT_REQUIRE_ABOVE_SMA200 = True  # SMA200上方を必須とするか

# JP売買ルール（全3区分バックテスト検証済み 2026-04-08）
# 統一パラメータ SL-5%/TP+40% が全区分で最良バランス:
#   Prime  500銘柄x5y (17,110件): EV+3.24%, PF2.20, 勝率43.0%
#   Standard 500銘柄x5y (16,011件): EV+2.88%, PF2.13, 勝率42.2%
#   Growth 478銘柄x5y (5,402件): EV+2.09%, PF1.57, 勝率24.9%
# RSフィルタ/GCフィルタは全区分で逆効果 → 不使用
# SMA200フィルタはPrime/Standardで+3.5〜4.3%改善 → ON維持
# BEAR期(2022)でもSMA200上ならEVプラス → regime抑制不要
BREAKOUT_STOP_LOSS = -0.05           # JP損切り-5%（全区分統一）
BREAKOUT_PROFIT_TARGET = 0.40        # JP標準利確+40%（全区分統一）
BREAKOUT_PROFIT_TARGET_EXTENDED = 0.25  # 延長利確+25%（急騰時8週保有判定）

# US専用売買ルール
# 根拠: us_mid 500銘柄x5y BTで検証。SL-20%/TP+15%が期待値+5.94%で最適
# 勝率65%, PF1.54, bear局面(2022)でも微プラス。R:R 1:0.75だが高勝率で補償
BREAKOUT_STOP_LOSS_US = -0.20        # US損切り-20%
BREAKOUT_PROFIT_TARGET_US = 0.15     # US利確+15%

# US通知フィルタ（★3以上のみ通知）
# 根拠: ★3+で平均リターンがランダムの上位3%ile。★4+はn不足(45件)で信頼性低い
BREAKOUT_US_PRE_MIN_QUALITY = 3

# BEAR相場専用パラメータ（10年BT 345件検証）
# BEAR全体ロング: 勝率48%, 平均-2.2%, PF=0.67 → 原則ロング非推奨
# Vol>=5xのみ例外: 勝率74%, 平均+2.8%, PF=1.67, ランダム上位3.7%ile
BREAKOUT_VOLUME_RATIO_US_BEAR = 5.0      # BEAR時の出来高閾値（通常3.0→5.0に引上げ）
# GCなしショート: 勝率62%, 平均+2.4%, PF=1.53, n=61 → ショート候補として通知
BREAKOUT_BEAR_SHORT_ENABLED = True        # BEAR時ショート候補通知を有効化      # USプレブレイクの最低品質スコア（0-5）

# 時価総額フィルタ（DUKE(新高値): 10倍株の92.4%が200億未満）
# 時価総額フィルタ（BT検証で撤廃: 大型株ほど好成績と判明 2026-04-08）
# JP: 500億超 EV+3.74%/PF2.44 vs 500億以下 EV+1.79%/PF1.59（Prime）
# US: $200B+ EV+4.15%/PF2.42 vs $10-50B EV+2.24%/PF1.56
# フィルタは有害 → daily_run.pyで無効化済み。下記は優先マーク用に残存
BREAKOUT_MAX_MARKET_CAP_JP = 20_000_000_000   # JP: 200億円（優先マーク用、フィルタ無効）
BREAKOUT_MAX_MARKET_CAP_JP_LOOSE = 50_000_000_000  # JP: 500億円（フィルタ無効）

# 有望セクター（DUKE統計: IT25.3%, サービス19%, 小売8%）
BREAKOUT_PREFERRED_SECTORS_JP = [
    "情報・通信業", "サービス業", "小売業",
]

# =============================================================================
# Earnings Acceleration 設定 (CAN SLIMの"C"と"A")
# =============================================================================

EA_MIN_PROFIT_GROWTH = 0.25          # 直近四半期の最低成長率: +25% (O'Neill準拠)
EA_MIN_ACCELERATION = 0.0            # 加速判定: 前期比成長率が改善していればOK
EA_MIN_CONSECUTIVE = 2               # 連続加速四半期数（最低2Q）
EA_MIN_REVENUE_GROWTH = 0.10         # 売上成長の最低ライン: +10%
EA_REQUIRE_REVENUE_VALIDATION = True  # 売上バリデーション有効化（O'Neillルール）

# =============================================================================
# 通知ルーティング設定
# =============================================================================
# strategy:market → SLACK_WEBHOOK_URL 環境変数名のマッピング
# 環境変数が未設定の場合は SLACK_WEBHOOK_URL にフォールバック
NOTIFY_CHANNELS = {
    "kuroten:JP":       "SLACK_WEBHOOK_KUROTEN_JP",
    "breakout:JP":      "SLACK_WEBHOOK_BREAKOUT_JP",
    "breakout:US":      "SLACK_WEBHOOK_BREAKOUT_US",
    "mega:US":          "SLACK_WEBHOOK_MEGA_US",
    "mega:JP":          "SLACK_WEBHOOK_MEGA_JP",
}
NOTIFY_FALLBACK_ENV = "SLACK_WEBHOOK_URL"

# =============================================================================
# Mega Strategy ($200B+) 専用設定
# =============================================================================
# BT検証 641件 (5年): BO限定 EV+11.29% PF20.54 勝率85%
# 全シグナル: EV+5.12% PF2.65 勝率65%
# BEAR 2022でも唯一プラス (EV+2.10%)。全天候型に最も近い戦略

MEGA_THRESHOLD_US = 200_000_000_000       # $200B — Mega判定閾値
MEGA_STOP_LOSS = -0.20                    # SL-20% (発動率5%、Megaは暴落しにくい)
MEGA_PROFIT_TARGET = 0.40                 # TP+40% (長く持つほど有利)
MEGA_PB_SUPPRESS_DAYS = 7                 # 同一銘柄PBの重複通知抑制期間（日）
MEGA_NOTIFY_ALSO_US_CHANNEL = True        # MegaシグナルをUS通常チャンネルにも送信

# =============================================================================
# JP MEGA ¥1兆+ 地力S/Aスコアリング戦略
# =============================================================================
# 10年BT検証: S/A EV+7.13%, 勝率69.2%, PF3.70
# 5年BT検証: S/A EV+9.74%, 勝率78.4%, PF6.81
# S/Aが優位だった年: 10/10年 (100%) — BEAR年含む

MEGA_JP_THRESHOLD = 1_000_000_000_000     # ¥1兆 — JP Mega判定閾値
MEGA_JP_STOP_LOSS = -0.20                 # SL-20% (5年・10年とも最適)
MEGA_JP_PROFIT_TARGET = 0.40              # TP+40% (5年・10年とも最適)

# Hybrid LH ローテーション設定
# BT検証 (WF 2022-2026): 確認3日+LH5日 → CAGR+40.4%, DD-23.1%, Sharpe1.46
MEGA_JP_CONFIRM_DAYS = 3                  # 新規切替に必要な連続TOP日数
MEGA_JP_LH_TRIGGER_DAYS = 5              # LHモード発動に必要な連続TOP日数
MEGA_JP_LH_ENABLED = True                # Hybrid LHローテーション有効化

# スコアリング重み
MEGA_JP_STRENGTH_WEIGHT = 0.4             # 地力スコアの重み
MEGA_JP_TIMING_WEIGHT = 0.6              # タイミングスコアの重み

# ランク閾値（総合スコア = 地力×0.4 + タイミング×0.6）
MEGA_JP_GRADE_S = 75                      # S(≥75): 最優先エントリー候補
MEGA_JP_GRADE_A = 55                      # A(≥55): 有力候補
MEGA_JP_GRADE_B = 40                      # B(≥40): 条件付きウォッチ

# 逆指値（買いストップ）注文管理
MEGA_JP_LIMIT_ORDER_EXPIRY_DAYS = 15     # SBI証券の逆指値最大有効期限（営業日）
MEGA_JP_REMINDER_INTERVAL_WEEKS = 2      # リマインダー間隔（週）

# =============================================================================
# 売却監視設定
# =============================================================================

SELL_MONITOR_DEFICIT_CHECK_INTERVAL = 7   # 赤字転落チェック間隔（日）
MARKET_REGIME_SUPPRESS_IN_BEAR = False    # BEAR時の買いシグナル抑制（オプション）

# =============================================================================
# Relative Strength (RS) ランキング設定
# =============================================================================

RS_LOOKBACK_DAYS = 126                   # RSルックバック期間（営業日、約6ヶ月）
RS_MIN_PERCENTILE_JP = 70               # JP: 上位30%以上のみ通過
RS_MIN_PERCENTILE_US = 70               # US: 上位30%以上のみ通過
RS_ENABLED = True                        # RSフィルタ有効/無効

# =============================================================================
# VCP (Volatility Contraction Pattern) 設定
# =============================================================================
# Minervini: 成功率90.77%（好環境時）, R:R 3:1以上

VCP_MIN_CONTRACTIONS = 2                 # 最低収縮回数
VCP_MAX_CONTRACTIONS = 6                 # 最大収縮回数
VCP_CONTRACTION_RATIO = 0.60            # 前回比60%以下で収縮判定
VCP_VOLUME_DRY_RATIO = 0.70             # 収縮中の出来高枯渇（平均70%以下）
VCP_BREAKOUT_VOLUME_SURGE = 1.4         # BO時出来高1.4倍以上
VCP_LOOKBACK_DAYS = 180                 # VCPパターン検出のルックバック期間（営業日）

# =============================================================================
# PEAD (Post-Earnings Announcement Drift) 設定
# =============================================================================
# Ball & Brown (1968), 日本市場でも残存効果あり

PEAD_MIN_SURPRISE_PCT = 0.20            # サプライズ下限20%
PEAD_HOLD_DAYS = 60                     # 保有期間60営業日
PEAD_TOP_N = 10                         # 上位10銘柄
PEAD_ENABLED_MONTHS = [1, 2, 4, 5, 7, 8, 10, 11]  # 決算月
PEAD_STOP_LOSS = -0.08                  # 損切り-8%
PEAD_PROFIT_TARGET = 0.15               # 利確+15%

# =============================================================================
# 上方修正ドリフト設定（日本市場特化）
# =============================================================================
# 日本市場最大のカタリスト、+15%/3ヶ月

REVISION_MIN_CHANGE_PCT = 0.10           # 修正幅下限10%
REVISION_CONSECUTIVE_BONUS = True        # 連続上方修正ボーナス
REVISION_HOLD_DAYS = 60                  # 保有期間60営業日
REVISION_STOP_LOSS = -0.08              # 損切り-8%
REVISION_PROFIT_TARGET = 0.20           # 利確+20%

# =============================================================================
# Weinstein Stage Analysis 設定
# =============================================================================
# 勝率69-80%, +13.2%/trade

STAGE_SMA_PERIOD = 150                  # 30週（150営業日）移動平均
STAGE_VOLUME_SURGE = 2.0                # Stage 2突入時の出来高倍率
STAGE_SLOPE_LOOKBACK = 10               # MA傾き判定のルックバック（営業日）
STAGE_MIN_SLOPE = 0.0                   # MA傾きの最低値（0以上=上向き）

# =============================================================================
# インサイダー・クラスター買い設定（US向け）
# =============================================================================
# SEC Form 4, 年率+4.8〜10.2%超過リターン

INSIDER_CLUSTER_WINDOW_DAYS = 10        # クラスター判定ウィンドウ（日）
INSIDER_MIN_BUYERS = 3                  # 最低購入者数
INSIDER_LOOKBACK_DAYS = 90              # 検索期間（日）
INSIDER_EXCLUDE_ROUTINE = True          # ルーチン取引除外

# =============================================================================
# 通知ルーティング拡張
# =============================================================================
NOTIFY_CHANNELS.update({
    "pead:JP":          "SLACK_WEBHOOK_KUROTEN_JP",
    "revision:JP":      "SLACK_WEBHOOK_KUROTEN_JP",
    "stage:JP":         "SLACK_WEBHOOK_BREAKOUT_JP",
    "stage:US":         "SLACK_WEBHOOK_BREAKOUT_US",
    "insider:US":       "SLACK_WEBHOOK_BREAKOUT_US",
})
