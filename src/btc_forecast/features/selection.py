"""
БЛОК 3 — Анализ признаков и формирование финального датасета (v6.0)

Изменения:
    - Переход к регрессии: целевая переменная = "fwd_ret".
    - mutual_info_classif заменён на mutual_info_regression.
    - Из KEEP_ALWAYS убран c_close (больше не нужен для обучения).
    - fwd_ret и target исключены из признаков, но target
      сохраняется в выходном датасете как мета-информация.
    - Статистика по годам показывает среднее и std fwd_ret.
"""

import json
import os
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_regression

# =============================================================================
# КОНФИГ
# =============================================================================
INPUT_1H = "features_1h.csv"
INPUT_6H = "features_6h.csv"

OUTPUT_1H = "final_dataset_1h.csv"
OUTPUT_6H = "final_dataset_6h.csv"

TARGET = "fwd_ret"

THRESHOLD_WEAK_RHO = 0.01
THRESHOLD_WEAK_MI = 0.003

CORR_DUP_THRESHOLD = 0.65

SCORE_WEIGHT_RHO = 0.5
SCORE_WEIGHT_MI = 0.5

MI_RANDOM_STATE = 42

# Колонки, не являющиеся признаками (таргет + метка классификации)
_NON_FEATURE = {"target", "fwd_ret"}

# Защищённые колонки (не удаляются weak/dup фильтрами)
KEEP_ALWAYS = [
    "volatility_1d",
    "volatility_7d",
    "atr_pct",
    "price_log",
    "price_rank_90d",
    "price_vs_sma_200d",
    "price_zscore_90d",
]


# =============================================================================
# 1. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================
def load_dataset(path: str, label: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"[{label}] не найден '{path}'. Сначала запустите FE."
        )
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index.name = "datetime"
    return df


def feature_list(df: pd.DataFrame) -> list:
    return [c for c in df.columns if c not in _NON_FEATURE]


def sentiment_columns(df: pd.DataFrame) -> list:
    return [c for c in df.columns if c.startswith("news_")]


def compute_feature_scores(df: pd.DataFrame, feats: list,
                           target_col: str) -> pd.DataFrame:
    rho = df[feats].corrwith(df[target_col], method="spearman").abs()
    r = df[feats].corrwith(df[target_col], method="pearson").abs()

    x = df[feats].fillna(0.0).values
    y = df[target_col].values
    mi_arr = mutual_info_regression(
        x, y, discrete_features=False, random_state=MI_RANDOM_STATE
    )
    mi = pd.Series(mi_arr, index=feats)

    scores = pd.DataFrame({"rho_abs": rho, "r_abs": r, "mi": mi})

    def _norm(s: pd.Series) -> pd.Series:
        rng = s.max() - s.min()
        if rng == 0 or np.isnan(rng):
            return pd.Series(0.0, index=s.index)
        return (s - s.min()) / rng

    rho_n = _norm(scores["rho_abs"])
    mi_n = _norm(scores["mi"])
    scores["score"] = SCORE_WEIGHT_RHO * rho_n + SCORE_WEIGHT_MI * mi_n
    scores = scores.sort_values("score", ascending=False)
    scores["rank"] = range(1, len(scores) + 1)
    return scores


def find_weak(scores: pd.DataFrame, keep_always: list) -> list:
    mask = (scores["rho_abs"] < THRESHOLD_WEAK_RHO) & (scores["mi"] < THRESHOLD_WEAK_MI)
    return [f for f in scores.index[mask] if f not in keep_always]


def _connected_components(nodes: list, edges: list) -> list:
    parent = {n: n for n in nodes}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for a, b in edges:
        union(a, b)

    groups = defaultdict(list)
    for n in nodes:
        groups[find(n)].append(n)
    return list(groups.values())


def find_duplicates(df: pd.DataFrame, feats: list, scores: pd.DataFrame,
                    already_drop: set, keep_always: list) -> tuple:
    candidates = [f for f in feats if f not in already_drop]
    fc = df[candidates].corr(method="pearson").abs()
    upper = fc.where(np.triu(np.ones(fc.shape), k=1).astype(bool))

    pairs = (
        upper.stack()
        .reset_index()
        .rename(columns={"level_0": "feat_1", "level_1": "feat_2", 0: "r"})
        .query("r > @CORR_DUP_THRESHOLD")
        .sort_values("r", ascending=False)
        .reset_index(drop=True)
    )

    if pairs.empty:
        return [], pairs, []

    edges = list(zip(pairs["feat_1"], pairs["feat_2"]))
    components = _connected_components(candidates, edges)

    drop_list = []
    group_decisions = []

    for comp in components:
        if len(comp) < 2:
            continue
        protected_in_group = [f for f in comp if f in keep_always]
        if protected_in_group:
            winner = protected_in_group[0]
            losers = [f for f in comp if f not in keep_always]
        else:
            comp_scores = scores.loc[[f for f in comp if f in scores.index], "score"]
            if comp_scores.empty:
                continue
            winner = comp_scores.idxmax()
            losers = [f for f in comp if f != winner]

        drop_list.extend(losers)
        group_decisions.append({
            "group_size": len(comp),
            "winner": winner,
            "winner_score": float(
                scores.loc[winner, "score"] if winner in scores.index else 0.0
            ),
            "losers": losers,
            "max_r_in_group": float(
                fc.loc[comp, comp].where(~np.eye(len(comp), dtype=bool)).max().max()
            ),
        })

    seen = set()
    drop_list_unique = []
    for f in drop_list:
        if f not in seen and f not in already_drop:
            seen.add(f)
            drop_list_unique.append(f)

    return drop_list_unique, pairs, group_decisions


def signal_by_year(df: pd.DataFrame, label: str, target_col: str) -> dict:
    print(f"\n  ── Статистика по годам [{label}] ──")
    tmp = df.copy()
    tmp["_yr"] = tmp.index.year
    by_year = {}

    for yr, g in tmp.groupby("_yr"):
        n = len(g)
        mean_ = float(g[target_col].mean())
        std_ = float(g[target_col].std())
        print(f"    {yr}: {n:5d} баров | fwd_ret mean={mean_:.5f} std={std_:.5f}")
        by_year[int(yr)] = {"n": int(n), "fwd_ret_mean": mean_, "fwd_ret_std": std_}
    return by_year


# =============================================================================
# 2. ОСНОВНАЯ ФУНКЦИЯ ОБРАБОТКИ
# =============================================================================
def process(label: str, in_path: str, out_path: str) -> dict | None:
    print("\n" + "=" * 72)
    print(f"  ТАЙМФРЕЙМ {label}")
    print("=" * 72)

    df = load_dataset(in_path, label)

    if TARGET not in df.columns:
        raise ValueError(f"[{label}] нет '{TARGET}'. Колонки с 'fwd_ret' отсутствуют.")

    feats = feature_list(df)
    sent_cols = sentiment_columns(df)
    keep_filters = KEEP_ALWAYS

    n = len(df)
    print(f"  Target:  fwd_ret (mean={df[TARGET].mean():.5f}, std={df[TARGET].std():.5f})")
    if "target" in df.columns:
        vc = df["target"].value_counts().sort_index()
        dist = "  ".join(f"{int(k):+d}: {v} ({v / n * 100:.1f}%)" for k, v in vc.items())
        print(f"  (3-класс target): {dist}")
    print(f"  Файл:    {in_path}")
    print(f"  Строк:   {n:,} | признаков: {len(feats)}")
    print(f"  Сентимент-фич на входе: {len(sent_cols)}")
    print(f"  Период:  {df.index[0]} — {df.index[-1]}")

    year_stats = signal_by_year(df, label, TARGET)

    print("\n  ── Скоринг признаков (MI_regression) ──")
    scores = compute_feature_scores(df, feats, TARGET)
    print("\n  Топ-20 по score:")
    print(scores.head(20).round(5).to_string())

    drop_weak = find_weak(scores, keep_filters)
    print(f"\n  ── Weak-фильтр (|ρ|<{THRESHOLD_WEAK_RHO} И MI<{THRESHOLD_WEAK_MI}) ──")
    print(f"  К удалению: {len(drop_weak)}")
    for f in sorted(drop_weak):
        print(
            f"    - {f:<42}  |ρ|={scores.loc[f, 'rho_abs']:.5f}  "
            f"MI={scores.loc[f, 'mi']:.5f}"
        )

    saved_by_mi = [
        f for f in scores.index
        if (scores.loc[f, "rho_abs"] < THRESHOLD_WEAK_RHO
            and scores.loc[f, "mi"] >= THRESHOLD_WEAK_MI
            and f not in keep_filters)
    ]
    if saved_by_mi:
        print(f"\n  ── Спасены по MI ({len(saved_by_mi)}) ──")
        for f in saved_by_mi:
            print(
                f"    + {f:<42}  |ρ|={scores.loc[f, 'rho_abs']:.5f}  "
                f"MI={scores.loc[f, 'mi']:.5f}"
            )

    drop_dup, pairs, group_decisions = find_duplicates(
        df, feats, scores, set(drop_weak), keep_filters
    )

    print(f"\n  ── Дедупликация (|r_pearson|>{CORR_DUP_THRESHOLD}) ──")
    print(f"  Пар выше порога: {len(pairs)}")
    print(f"  Групп связности: {len(group_decisions)}")
    print(f"  К удалению: {len(drop_dup)}")
    for dec in group_decisions:
        print(
            f"    группа {dec['group_size']}, max|r|={dec['max_r_in_group']:.3f}: "
            f"оставлен {dec['winner']} (score={dec['winner_score']:.3f}), "
            f"удалены {dec['losers']}"
        )

    all_nan = [c for c in df.columns if df[c].isna().all()]
    if all_nan:
        print(f"\n  Удалено (100% NaN): {len(all_nan)}: {all_nan}")

    drop_all = set(drop_weak) | set(drop_dup) | set(all_nan)
    present_meta = [c for c in KEEP_ALWAYS if c in df.columns and c not in all_nan]

    vol_required = ["volatility_1d", "volatility_7d", "atr_pct"]
    missing_vol = [c for c in vol_required if c not in present_meta]
    if missing_vol:
        print(f"\n  ⚠ vol-индикаторы отсутствуют: {missing_vol}")
    else:
        print(f"\n  ✓ vol-индикаторы защищены: {vol_required}")

    price_ctx = ["price_log", "price_rank_90d", "price_vs_sma_200d", "price_zscore_90d"]
    missing_price = [c for c in price_ctx if c not in present_meta]
    if missing_price:
        print(f"  ⚠ ценовой контекст отсутствует: {missing_price}")
    else:
        print(f"  ✓ ценовой контекст защищён: {price_ctx}")

    final_feats = [f for f in feats if f not in drop_all and f not in present_meta]

    # Выходной список колонок: meta + final_feats + fwd_ret + target (если есть)
    out_cols = present_meta + final_feats + [TARGET]
    if "target" in df.columns and "target" not in out_cols:
        out_cols.append("target")

    seen_cols = set()
    out_cols_dedup = []
    for c in out_cols:
        if c in df.columns and c not in seen_cols:
            out_cols_dedup.append(c)
            seen_cols.add(c)

    df_out = df[out_cols_dedup].copy()
    n_before = len(df_out)
    df_out.ffill(inplace=True)
    df_out.dropna(inplace=True)
    n_dropped = n_before - len(df_out)
    if n_dropped:
        print(f"  Удалено строк с NaN: {n_dropped} → осталось {len(df_out):,}")

    if len(df_out) == 0:
        print("  ❌ Датасет пуст после очистки — сохранение пропущено")
        return None

    df_out.to_csv(out_path, index=True, index_label="datetime")

    scores_path = out_path.replace("final_dataset_", "feature_scores_")
    scores_out = scores.copy()
    scores_out["status"] = "kept"
    scores_out.loc[scores_out.index.isin(drop_weak), "status"] = "dropped_weak"
    scores_out.loc[scores_out.index.isin(drop_dup), "status"] = "dropped_duplicate"
    scores_out.loc[scores_out.index.isin(all_nan), "status"] = "dropped_all_nan"
    scores_out.loc[scores_out.index.isin(present_meta), "status"] = "meta"
    scores_out.to_csv(scores_path, index_label="feature")

    report_path = out_path.replace("final_dataset_", "selection_report_").replace(".csv", ".json")
    target_stats = {
        "mean": float(df[TARGET].mean()),
        "std": float(df[TARGET].std()),
        "min": float(df[TARGET].min()),
        "max": float(df[TARGET].max()),
    }

    report = {
        "label": label,
        "target_col": TARGET,
        "input": in_path,
        "output": out_path,
        "params": {
            "threshold_weak_rho": THRESHOLD_WEAK_RHO,
            "threshold_weak_mi": THRESHOLD_WEAK_MI,
            "corr_dup_threshold": CORR_DUP_THRESHOLD,
            "score_weight_rho": SCORE_WEIGHT_RHO,
            "score_weight_mi": SCORE_WEIGHT_MI,
        },
        "input_stats": {
            "n_rows": int(n),
            "n_features": len(feats),
            "period_start": str(df.index[0]),
            "period_end": str(df.index[-1]),
            "target_distribution": target_stats,
            "by_year": year_stats,
        },
        "selection": {
            "dropped_weak": sorted(drop_weak),
            "saved_by_mi": sorted(saved_by_mi),
            "dropped_duplicate": sorted(drop_dup),
            "dropped_all_nan": sorted(all_nan),
            "group_decisions": group_decisions,
            "kept_features": final_feats,
            "meta_columns": present_meta,
            "sentiment_cols": sorted(sent_cols),
            "sentiment_kept": sorted(c for c in sent_cols if c not in drop_all),
        },
        "output_stats": {
            "n_rows": int(len(df_out)),
            "n_features_final": len(final_feats),
            "n_meta": len(present_meta),
        },
    }
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)

    print(f"\n  ── Итог [{label}] ──")
    print(f"  Исходных признаков:   {len(feats)}")
    print(f"  Удалено weak:         {len(drop_weak)}")
    print(f"  Спасено по MI:        {len(saved_by_mi)}")
    print(f"  Удалено дубликатов:   {len(drop_dup)}")
    print(f"  Удалено all-NaN:      {len(all_nan)}")
    print(f"  Финальных фич:        {len(final_feats)}")
    print(f"  Мета-колонок:         {len(present_meta)}")
    sent_kept = [c for c in sent_cols if c not in drop_all]
    print(f"  Сентимент-фич (без защиты): {len(sent_kept)}/{len(sent_cols)}")
    print(f"  Строк:                {len(df_out):,}")
    print(f"  Период:               {df_out.index[0]} — {df_out.index[-1]}")
    print(f"  Сохранено: {out_path}, {scores_path}, {report_path}")

    return {
        "label": label,
        "n_input": len(feats),
        "n_weak": len(drop_weak),
        "n_saved_mi": len(saved_by_mi),
        "n_dup": len(drop_dup),
        "n_nan": len(all_nan),
        "n_final": len(final_feats),
        "n_meta": len(present_meta),
        "n_sent": len(sent_kept),
        "n_rows": len(df_out),
        "feats": final_feats,
    }


# =============================================================================
# 3. ЗАПУСК
# =============================================================================
if __name__ == "__main__":
    tasks = [
        ("1H", INPUT_1H, OUTPUT_1H),
        ("6H", INPUT_6H, OUTPUT_6H),
    ]

    results = {}
    for label, in_path, out_path in tasks:
        if not os.path.exists(in_path):
            print(f"\n  ⚠ {in_path} не найден — датасет {label} пропущен")
            continue
        res = process(label, in_path, out_path)
        if res is not None:
            results[label] = res

    if len(results) == 2:
        print("\n" + "=" * 80)
        print("  СРАВНЕНИЕ ДАТАСЕТОВ")
        print("=" * 80)
        hdr = f"  {'Метрика':<25}" + "".join(f"{lbl:>13}" for lbl in results)
        print(hdr)
        print("  " + "-" * (25 + 13 * len(results)))

        metrics = [
            ("n_input", "Исходных признаков"),
            ("n_weak", "Удалено (weak)"),
            ("n_saved_mi", "Спасено по MI"),
            ("n_dup", "Удалено (дубл.)"),
            ("n_nan", "Удалено (NaN)"),
            ("n_final", "Финальных фич"),
            ("n_meta", "Мета-колонок"),
            ("n_sent", "Сентимент-фич"),
            ("n_rows", "Строк"),
        ]
        for key, desc in metrics:
            row = f"  {desc:<25}"
            for lbl in results:
                v = results[lbl][key]
                row += f"{v:>13,}" if key == "n_rows" else f"{v:>13}"
            print(row)

        keys = list(results.keys())
        s1 = set(results[keys[0]]["feats"])
        s2 = set(results[keys[1]]["feats"])
        print(f"\n  [{keys[0]} vs {keys[1]}] "
              f"только в {keys[0]} ({len(s1 - s2)}): {sorted(s1 - s2)}")
        print(f"  [{keys[0]} vs {keys[1]}] "
              f"только в {keys[1]} ({len(s2 - s1)}): {sorted(s2 - s1)}")
        print(f"  [{keys[0]} vs {keys[1]}] "
              f"общих ({len(s1 & s2)}): {sorted(s1 & s2)}")

        print("\n" + "=" * 80)
        print("  Готово. Выходные файлы:")
        for label, _, out_path in tasks:
            if label in results:
                r = results[label]
                print(f"    {out_path}  ({r['n_final']} фич + {r['n_meta']} мета)")
        print("=" * 80)
    else:
        print("\nОбработано менее двух датасетов, сравнение невозможно.")