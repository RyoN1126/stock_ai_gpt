# 📈 4H Pullback Scanner（morning / close 2セッション運用）

本プロジェクトは、日本株向けの

* 4時間足押し目シグナル生成
* セッション別（morning / close）運用
* TP / SL による機械的判定
* 週次・期間別の統計集計

を行う検証用ツールです。

---

## 🧠 運用コンセプト

### セッションは2種類のみ

| session | 実行タイミング       | 目的         |
| ------- | ------------- | ---------- |
| morning | 前場終了後（11:30〜） | 当日後場で買うか判断 |
| close   | 引け後（15:30〜）   | 翌日前場で買うか判断 |

時間ではなく「セッション単位」で管理します。

---

## 📂 ディレクトリ構成

```
project_root/
├── main.py
├── evaluate_results.py
├── aggregate.py
├── tse_listed_issues.xlsx
│
└── outputs/
    ├── signals/
    │   ├── 2026-02-27_morning.json
    │   └── 2026-02-27_close.json
    │
    ├── results/
    │   ├── 2026-02-27_morning_result.json
    │   └── 2026-02-27_close_result.json
    │
    └── aggregates/
```

---

## 🚀 使い方

### ① シグナル生成

#### 前場終了後

```bash
python main.py --session morning
```

出力：

```
outputs/signals/YYYY-MM-DD_morning.json
```

---

#### 引け後

```bash
python main.py --session close
```

出力：

```
outputs/signals/YYYY-MM-DD_close.json
```

---

### ② 判定（WIN / LOSS / OPEN）

#### 特定ファイルを判定

```bash
python evaluate_results.py --input outputs/signals/2026-02-27_morning.json
```

出力：

```
outputs/results/2026-02-27_morning_result.json
```

---

#### latestを使う

```bash
python evaluate_results.py --latest-meta
```

---

## 判定ルール

| session | 判定開始時刻       |
| ------- | ------------ |
| morning | 当日 12:30以降   |
| close   | 翌営業日 09:00以降 |

その時点から：

* TPに先に到達 → WIN
* SLに先に到達 → LOSS
* 未到達 → OPEN
* 同一足で両方到達 → AMBIG

---

### ③ 集計

#### 全期間

```bash
python aggregate.py
```

---

#### 週次

```bash
python aggregate.py --week 2026-W09
```

---

#### 日次スナップショット作成

```bash
python aggregate.py --daily
```

---

## 📊 出力される統計

* 合計R
* 勝率（WIN / (WIN+LOSS)）
* 平均R（確定分のみ）
* OPEN比率
* 最大連敗
* session別成績（morning / close）

---

## 🔍 シグナルロジック概要

* 4H EMA20押し目
* 1H足で出来高＋陽線確認
* ATRベースSL
* RR固定（現在2.0）

---

## ⚠ 注意点

* 翌営業日の判定は土日のみスキップ（祝日は未対応）
* yfinanceの分足取得には制限あり
* 1分足は直近7日程度まで


