@echo off
REM 黒字転換スクリーニング定期実行用バッチ
REM タスクスケジューラから呼び出す想定
REM
REM 設定例（タスクスケジューラ）:
REM   トリガー: 毎月15日 / 四半期決算発表後（2月, 5月, 8月, 11月）
REM   操作: このバッチファイルを実行
REM   開始場所: C:\Tools\inv_kuroten

cd /d C:\Tools\inv_kuroten

REM Python実行（Slack通知付き）
python main.py

REM 終了コードを返す
exit /b %ERRORLEVEL%
