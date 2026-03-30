"""
黒字転換2倍株 共通設定パラメータ

スクリーニング・バックテスト・フィルタの全設定を一元管理する。
各モジュールはここからimportして使用すること。
"""

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
STOP_LOSS_PCT = -0.20               # 損切りライン（-20%）※黒字転換戦略はボラが高く-10%だと早期損切り多発（BT検証済）
TRAILING_STOP_TRIGGER = 0.80        # トレーリングストップ発動（+80%到達時）
TRAILING_STOP_PCT = -0.20           # 高値からの下落で利確（-20%）
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
REC_V2_YOY_SAME_Q_RED_BONUS = 2          # 前年同期も赤字 → 構造的改善(+2)
REC_V2_PROFIT_MCAP_HIGH = 0.02           # 利益/時価総額 > 2% → +2
REC_V2_PROFIT_MCAP_LOW = 0.005           # 利益/時価総額 > 0.5% → +1
REC_V2_REVENUE_GROWTH_HIGH = 0.15        # 前年同期比売上 > +15% → +2
REC_V2_REVENUE_GROWTH_LOW = 0.05         # 前年同期比売上 > +5% → +1
REC_V2_FORECAST_ALIGNED_BONUS = 1        # 通期予想黒字+進捗率健全 → +1

# --- 減点ファクター ---
REC_V2_SEASONAL_PENALTY_MILD = -2        # 前年同期が黒字(1年)
REC_V2_SEASONAL_PENALTY_STRONG = -3      # 前年同期が黒字(2年以上)
REC_V2_PRIOR_FAILURE_PENALTY = -2        # 同銘柄の過去シグナル失敗
REC_V2_THIN_PROFIT_SEVERE = -2           # 営業利益 < 1億
REC_V2_THIN_PROFIT_MILD = -1             # 営業利益 < 3億
REC_V2_DEPTH_MISMATCH_PENALTY = -1       # 5Q+赤字で回復が弱い

# --- 重み削減（旧ファクター） ---
REC_V2_CONSECUTIVE_RED_BONUS = 1         # 連続赤字3Q以上: +1（旧: 最大+4）
REC_V2_DOUBLE_TURN_BONUS = 1             # ダブル転換: +1（旧: +2）

# --- F-Score連携 ---
REC_V2_FSCORE_HIGH = 5                   # F-Score >= 5 → +1（財務健全）
REC_V2_FSCORE_LOW = 2                    # F-Score <= 2 → -1（財務懸念）

# --- グレード閾値（スコア範囲: 約-7〜+7） ---
REC_V2_GRADE_S = 5                       # S(Top Pick): 年3-5件の厳選投資対象
REC_V2_GRADE_A = 3                       # A(Strong): 枠があれば投資
REC_V2_GRADE_B = 1                       # B(Watch): ウォッチのみ

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
BREAKOUT_REQUIRE_ABOVE_SMA200 = True  # SMA200上方を必須とするか

# ブレイクアウト売買ルール（書籍コンセンサス: オニール/DUKE(ガチ)/kenmo）
BREAKOUT_STOP_LOSS = -0.10           # 損切り-10%（DUKE(ガチ)準拠、6冊コンセンサス-8〜10%帯）
BREAKOUT_PROFIT_TARGET = 0.20        # 標準利確+20%（オニール/DUKE(ガチ)コンセンサス）
BREAKOUT_PROFIT_TARGET_EXTENDED = 0.25  # 延長利確+25%（急騰時8週保有判定）

# 時価総額フィルタ（DUKE(新高値): 10倍株の92.4%が200億未満）
BREAKOUT_MAX_MARKET_CAP_JP = 20_000_000_000   # JP: 200億円（優先）
BREAKOUT_MAX_MARKET_CAP_JP_LOOSE = 50_000_000_000  # JP: 500億円（許容上限）

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
}
NOTIFY_FALLBACK_ENV = "SLACK_WEBHOOK_URL"
