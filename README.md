

# stock_ai_gpt
日本株向けの検証用ツールです。

- 4H押し目（EMA20）＋ 1H出来高確認で「候補」を生成
- morning / close の2セッション運用
- TP / SL で機械的に評価（WIN / LOSS / OPEN / NOT_FILLED）
- 期間集計（aggregate）

> 注意: 本ツールは「検証用」です。実売買の執行・手数料・スリッページ等は最小限の仮定であり、必要に応じて拡張してください。

---

## セッション運用
| session | 実行タイミング(JST) | cutoff | 用途 |
|---|---:|---:|---|
| morning | 前場終了後 | 11:30 | 当日後場の検討 |
| close | 引け後 | 15:30 | 翌日前場の検討 |

---

## ディレクトリ構成（実装準拠）
```

project_root/
├── main.py
├── evaluate_results.py
├── aggregate.py
├── tse_listed_issues.xlsx            # Universe（推奨: JPX/TSEの listed issues）
├── data_j.xlsx                       # Universe（fallback）
├── signals/
│   └── signals_YYYY-MM-DD_session.json
├── outputs/
│   ├── today_candidates_latest.json
│   └── today_candidates_latest_meta.json
└── results/
└── result_YYYY-MM-DD_session_YYYYmmdd-HHMMSSZ.json

````

---

## 1) シグナル生成（main.py）

### 実行例
```bash
# 前場後
python main.py --session morning

# 引け後
python main.py --session close

# 過去日付を指定してバックフィル（JST日付）
python main.py --session close --date 2026-02-27
````

### 主なオプション

* `--session {morning,close}` 必須
* `--date YYYY-MM-DD`（JSTの日付。省略時は当日JST）
* `--market_filter {on,off}`（デフォルトon）
* `--market_ticker`（デフォルト `^N225`）
* `--max_positions`（デフォルト 2）
* `--rr`（デフォルト 1.8）
* `--atr_mult`（デフォルト 1.0）
* `--account_size`（デフォルト 200000 円）
* `--risk_per_trade`（デフォルト 0.01）
* `--lot_size`（デフォルト 100）
* `--universe_xlsx`（Universeファイルを上書き指定）

### 出力

* `signals/signals_YYYY-MM-DD_session.json`

  * 既に存在する場合、上書きせず `..._HHMMSS.json` を作成します
* `outputs/today_candidates_latest.json`（常に最新に上書き）
* `outputs/today_candidates_latest_meta.json`（常に最新に上書き）

---

## 2) 評価（evaluate_results.py）

シグナル（entry/sl/tp）に対し、1時間足で TP/SL のどちらが先に触れたかを評価します。

### 実行例

```bash
# latest（outputs/today_candidates_latest_meta.json）から対象signalsを特定して評価
python evaluate_results.py --latest

# 日付＋セッションで評価（signals/signals_YYYY-MM-DD_session*.json を解決）
python evaluate_results.py --date 2026-02-27 --session close
```

### 主なオプション

* `--latest` または `--date`（どちらか必須）
* `--session {morning,close}`（`--date` のとき必須）
* `--signals_dir`（デフォルト `signals`）
* `--results_dir`（デフォルト `results`）
* `--latest_meta`（デフォルト `outputs/today_candidates_latest_meta.json`）
* `--hold_days`（デフォルト 5）

  * バックフィル時の未来リーク防止のため、評価窓を最大日数で制限します
* `--account_size` / `--lot_size`

  * 約定可能性のチェックに使用（100株単位など）
* `--allow_unfilled_entry`

  * 互換用: entry到達を必須にしない（デフォルトは「entry到達必須」＝未到達は NOT_FILLED）

### 判定ルール（実装準拠）

* `entry` が到達しない場合 → `NOT_FILLED`

  * High/Low がある場合: `Low <= entry <= High` を満たすと約定扱い
* 約定後:

  * 同一1Hバーで SL と TP を両方触れた場合 → **保守的に LOSS 扱い**
  * それ以外は先に触れた方で `WIN` / `LOSS`
* 期間内にどちらも触れない → `OPEN`

### 出力

* `results/result_YYYY-MM-DD_session_YYYYmmdd-HHMMSSZ.json`

  * `summary`（勝率/総R/総PnLなど）
  * `results[]`（銘柄ごとの結果、pnl_yen、R、shares_used など）

---

## 3) 集計（aggregate.py）

### 実行例

```bash
# デフォルト: (date, session) ごとに最新1件だけを集計して安定表示
python aggregate.py

# results_dir配下の result_*.json を間引かず全件集計
python aggregate.py --all_files

# 期間指定
python aggregate.py --from 2026-02-01 --to 2026-02-28
```

### オプション

* `--results_dir`（デフォルト `results`）
* `--all_files`（通常は日付×sessionごとに最新1件へdedupe）
* `--from YYYY-MM-DD`（inclusive）
* `--to YYYY-MM-DD`（inclusive）

### 出力（標準出力）

* 総トレード数 / WIN・LOSS・OPEN・NOT_FILLED
* 勝率（解決のみ）
* FillRate（NOT_FILLED除く比率）
* 合計R / 合計PnL(円)

---

## 注意点

* yfinanceのデータ制限・欠損により、銘柄によっては `no_data_1h` や `NOT_FILLED` が増える場合があります
* 日本の祝日・休場日は、日付指定バックフィル時にデータ欠損が起きやすいので、必要なら「営業日カレンダー」導入を推奨します

```

---

## 次にやると良い README 改善（任意）
READMEが“使える”状態になったら、さらに良くするなら：
- `signals` JSON のフィールド例（`market_info / drop_stats / sizing / candidates`）を載せる  
- `results` JSON のフィールド例（`meta / summary / results[]`）を載せる  
- GitHub Actions（実行タイミング、Pages反映、results-history運用）を「運用手順」として追記

---

必要なら、**READMEを「差分（旧→新）」の形**でも出すし、GitHub上でそのままコミットできるように `README.md` ファイルとして整形した内容も渡せるよ。
::contentReference[oaicite:7]{index=7}
```

[1]: https://raw.githubusercontent.com/RyoN1126/stock_ai_gpt/main/main.py "raw.githubusercontent.com"
[2]: https://raw.githubusercontent.com/RyoN1126/stock_ai_gpt/main/evaluate_results.py "raw.githubusercontent.com"
[3]: https://raw.githubusercontent.com/RyoN1126/stock_ai_gpt/main/aggregate.py "raw.githubusercontent.com"
