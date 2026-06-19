"""Блок обучения — каскад GATE -> регрессор-на-скачках.

GATE: бинарная классификация "будет движение / нет" на всех барах
(final_dataset_*h.csv). Кандидаты XGB / LightGBM, выбор по AUC.

REG: регрессия по fwd_ret только на скачках + ценовой контекст
(final_dataset_*h_jump.csv). Кандидаты xgb_huber / lgbm_huber /
hgb_mae / elasticnet, выбор по MAE.

Инференс: casc = sign(reg) если |reg| >= tau и gate == 1. На барах
без предсказания регрессора reg = 0 -> casc = 0 (флэт). Режимы tau:
static (фиксированный порог) и dynamic (порог = K * vol(t-1)).
"""

import json
import os
import pickle
import warnings

import numpy as np
import pandas as pd
from scipy.stats import randint, uniform
from sklearn.base import clone
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import ElasticNet
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    matthews_corrcoef,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import RandomizedSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier, XGBRegressor

try:
    from lightgbm import LGBMClassifier, LGBMRegressor
    _HAS_LGBM = True
except ImportError:
    _HAS_LGBM = False

warnings.filterwarnings("ignore")

DATA_1H = "final_dataset_1h.csv"
DATA_6H = "final_dataset_6h.csv"
DATA_1H_JUMP = "final_dataset_1h_jump.csv"
DATA_6H_JUMP = "final_dataset_6h_jump.csv"

LOAD_TUNED_PARAMS = True
TUNED_PARAMS_FILE_GATE = "best_params.json"
TUNED_PARAMS_FILE_REG = "best_params_reg.json"
REG_PARAMS_KEY = "dir_jump"

FWD_COL = "fwd_ret"
TRAIN_TARGET = "t_static"
EVAL_MODES = ["static", "dynamic"]

STATIC_THRESH_1H = 0.008
STATIC_THRESH_6H = 0.015
DYNAMIC_K_1H = 1.0
DYNAMIC_K_6H = 1.0
VOL_INDICATOR_1H = "volatility_1d"
VOL_INDICATOR_6H = "volatility_7d"
VOL_FALLBACK = "atr_pct"

TEST_RATIO = 0.2
N_WF_SPLITS = 5
PURGE_1H = 1
PURGE_6H = 6
RANDOM_STATE = 42

N_ITER_SEARCH = 30
TUNE_MODELS = True

GATE_SELECTION_METRIC = "auc"
DIR_SELECTION_METRIC = "mae"

TAU_GRID_SIZE = 41
TAU_MAX_QUANTILE = 0.99

HUBER_DELTA_1H = 0.006
HUBER_DELTA_6H = 0.015

_LEAK_PREFIX = ("fwd_ret", "target", "t_static", "t_dynamic")
_NON_FEATURE = {"c_close"}
_PRICE_CONTEXT_COLS = {
    "price_log",
    "price_rank",
    "price_vs_sma_ratio",
    "price_zscore",
}

_TUNED_GATE = None
_TUNED_REG = None


def _load_tuned(path, label):
    if not LOAD_TUNED_PARAMS or not os.path.exists(path):
        if LOAD_TUNED_PARAMS:
            print(f"  warn {label}: '{path}' не найден -> дефолты")
        return None
    with open(path) as f:
        data = json.load(f)
    meta = data.get("_meta", {})
    print(
        f"  {label}: загружено из '{path}' "
        f"(бюджет='{meta.get('budget', '?')}', "
        f"trials={meta.get('n_trials_per_combo', '?')})"
    )
    return data.get("results", {})


def _resolve_path(path, label):
    if not os.path.exists(path):
        raise FileNotFoundError(f"[{label}] не найден '{path}'")
    print(f"  [{label}] вход: {path}")
    return path


def _get_vol(df, preferred):
    if preferred in df.columns:
        return df[preferred]
    if VOL_FALLBACK in df.columns:
        print(f"  warn '{preferred}' нет -> fallback '{VOL_FALLBACK}'")
        return df[VOL_FALLBACK]
    raise KeyError(f"Нет '{preferred}' и '{VOL_FALLBACK}'")


def build_targets(df, vol_indicator, dynamic_k, static_thresh):
    """Добавляет t_static (для GATE) и t_dynamic (для подбора tau)."""
    if FWD_COL not in df.columns:
        raise KeyError(f"Нет колонки '{FWD_COL}'")
    df = df.dropna(subset=[FWD_COL]).copy()
    fwd = df[FWD_COL]
    df["t_static"] = np.where(
        fwd > static_thresh, 1, np.where(fwd < -static_thresh, -1, 0)
    )
    vol_shift = _get_vol(df, vol_indicator).shift(1)
    valid = vol_shift.notna() & (vol_shift > 0)
    df = df.loc[valid].copy()
    vs = vol_shift.loc[valid]
    fw = df[FWD_COL]
    df["t_dynamic"] = np.where(
        fw > dynamic_k * vs, 1, np.where(fw < -dynamic_k * vs, -1, 0)
    )
    return df, vs


def split_features(df):
    """Признаки для gate (без leakage и мета-колонок)."""
    return [
        c
        for c in df.columns
        if c not in _NON_FEATURE and not c.startswith(_LEAK_PREFIX)
    ]


def split_features_jump(df):
    """Признаки для регрессора (включают price_*, но не таргеты)."""
    return [
        c
        for c in df.columns
        if c not in _NON_FEATURE and not c.startswith(_LEAK_PREFIX)
    ]


def make_gate_xy(df, target_col, feat_cols):
    """Gate: все бары, y=1 если движение."""
    x = df[feat_cols].copy()
    y = (df[target_col] != 0).astype(int)
    ok = x.notna().all(axis=1)
    return x.loc[ok], y.loc[ok]


def make_reg_jump_xy(df_jump, feat_cols_jump):
    """REG: только скачки, y=fwd_ret."""
    x = df_jump[feat_cols_jump].copy()
    y = df_jump[FWD_COL].copy()
    ok = x.notna().all(axis=1) & y.notna()
    return x.loc[ok], y.loc[ok]


def purged_wf_indices(n, n_splits, purge):
    fold = n // (n_splits + 1)
    for k in range(1, n_splits + 1):
        cut = fold * k
        tr_end = cut - purge
        va_end = min(cut + fold, n)
        if tr_end <= 0 or va_end <= cut:
            continue
        yield np.arange(0, tr_end), np.arange(cut, va_end)


class PurgedWalkForward:
    def __init__(self, n_splits, purge):
        self.n_splits = n_splits
        self.purge = purge

    def split(self, x, y=None, groups=None):
        yield from purged_wf_indices(len(x), self.n_splits, self.purge)

    def get_n_splits(self, x=None, y=None, groups=None):
        return self.n_splits


def _spw(y):
    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    return (neg / pos) if pos > 0 else 1.0


def gate_specs(spw):
    specs = {
        "xgb": {
            "estimator": XGBClassifier(
                objective="binary:logistic",
                eval_metric="logloss",
                scale_pos_weight=spw,
                random_state=RANDOM_STATE,
                n_jobs=-1,
                tree_method="hist",
            ),
            "param_dist": {
                "n_estimators": randint(150, 500),
                "max_depth": randint(3, 8),
                "learning_rate": uniform(0.01, 0.15),
                "subsample": uniform(0.6, 0.4),
                "colsample_bytree": uniform(0.6, 0.4),
                "min_child_weight": randint(1, 10),
            },
        },
    }
    if _HAS_LGBM:
        specs["lgbm"] = {
            "estimator": LGBMClassifier(
                objective="binary",
                is_unbalance=True,
                random_state=RANDOM_STATE,
                n_jobs=-1,
                verbose=-1,
            ),
            "param_dist": {
                "n_estimators": randint(150, 500),
                "num_leaves": randint(15, 80),
                "max_depth": [-1, 4, 6, 8, 12],
                "learning_rate": uniform(0.01, 0.15),
                "subsample": uniform(0.6, 0.4),
            },
        }
    return specs


def reg_specs(huber_delta):
    specs = {
        "xgb_huber": {
            "estimator": XGBRegressor(
                objective="reg:pseudohubererror",
                huber_slope=huber_delta,
                n_estimators=400,
                max_depth=5,
                learning_rate=0.03,
                subsample=0.85,
                colsample_bytree=0.85,
                reg_alpha=0.1,
                reg_lambda=1.0,
                tree_method="hist",
                n_jobs=-1,
                random_state=RANDOM_STATE,
            ),
            "param_dist": {
                "n_estimators": randint(200, 700),
                "max_depth": randint(3, 8),
                "learning_rate": uniform(0.01, 0.10),
                "subsample": uniform(0.6, 0.4),
                "colsample_bytree": uniform(0.6, 0.4),
                "reg_alpha": uniform(0.001, 1.0),
                "reg_lambda": uniform(0.1, 5.0),
            },
        },
        "hgb_mae": {
            "estimator": HistGradientBoostingRegressor(
                loss="absolute_error",
                max_iter=400,
                max_depth=6,
                learning_rate=0.03,
                l2_regularization=1.0,
                random_state=RANDOM_STATE,
            ),
            "param_dist": {
                "max_iter": randint(200, 700),
                "max_depth": [3, 5, 6, 8, None],
                "learning_rate": uniform(0.01, 0.10),
                "min_samples_leaf": randint(20, 100),
                "l2_regularization": uniform(0.1, 5.0),
            },
        },
        "elasticnet": {
            "estimator": Pipeline(
                [
                    ("scaler", StandardScaler()),
                    (
                        "model",
                        ElasticNet(
                            alpha=0.001,
                            l1_ratio=0.5,
                            max_iter=5000,
                            random_state=RANDOM_STATE,
                        ),
                    ),
                ]
            ),
            "param_dist": {
                "model__alpha": uniform(0.0001, 0.05),
                "model__l1_ratio": uniform(0.1, 0.8),
            },
        },
    }
    if _HAS_LGBM:
        specs["lgbm_huber"] = {
            "estimator": LGBMRegressor(
                objective="huber",
                alpha=huber_delta,
                n_estimators=400,
                num_leaves=31,
                learning_rate=0.03,
                subsample=0.85,
                subsample_freq=1,
                colsample_bytree=0.85,
                reg_alpha=0.1,
                reg_lambda=1.0,
                min_data_in_leaf=30,
                random_state=RANDOM_STATE,
                n_jobs=-1,
                verbose=-1,
            ),
            "param_dist": {
                "n_estimators": randint(200, 700),
                "num_leaves": randint(15, 80),
                "max_depth": [-1, 4, 6, 8, 12],
                "learning_rate": uniform(0.01, 0.10),
                "subsample": uniform(0.6, 0.4),
                "colsample_bytree": uniform(0.6, 0.4),
                "reg_alpha": uniform(0.001, 1.0),
                "reg_lambda": uniform(0.1, 5.0),
                "min_data_in_leaf": randint(10, 80),
            },
        }
    return specs


def tune_candidate(spec, x_tr, y_tr, cv, scoring):
    if not TUNE_MODELS:
        return clone(spec["estimator"])
    search = RandomizedSearchCV(
        estimator=spec["estimator"],
        param_distributions=spec["param_dist"],
        n_iter=N_ITER_SEARCH,
        cv=cv,
        scoring=scoring,
        n_jobs=-1,
        random_state=RANDOM_STATE,
        verbose=0,
        error_score="raise",
    )
    search.fit(x_tr, y_tr)
    return search.best_estimator_


def _binary_metrics(y_true, y_pred, y_proba):
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    n_cls = len(np.unique(y_true))
    out = {
        "n": len(y_true),
        "acc": accuracy_score(y_true, y_pred),
        "f1_macro": f1_score(
            y_true, y_pred, average="macro", zero_division=0
        ),
        "prec_pos": precision_score(
            y_true, y_pred, pos_label=1, zero_division=0
        ),
        "rec_pos": recall_score(
            y_true, y_pred, pos_label=1, zero_division=0
        ),
    }
    if n_cls < 2:
        out["mcc"] = float("nan")
        out["auc"] = float("nan")
    else:
        out["mcc"] = matthews_corrcoef(y_true, y_pred)
        try:
            out["auc"] = roc_auc_score(y_true, y_proba)
        except ValueError:
            out["auc"] = float("nan")
    return out


def _reg_metrics(y_true, y_pred):
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    if len(y_true) == 0:
        return {
            "n": 0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "sign_acc": float("nan"),
        }
    mae = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mask = np.abs(y_true) > 1e-9
    sign_acc = (
        float(np.mean(np.sign(y_pred[mask]) == np.sign(y_true[mask])))
        if mask.sum() > 0
        else float("nan")
    )
    return {"n": len(y_true), "mae": mae, "rmse": rmse, "sign_acc": sign_acc}


def wf_oos_clf(estimator, x, y, cv):
    yt, yp, ypr, idx = [], [], [], []
    for tr, va in purged_wf_indices(len(x), cv.n_splits, cv.purge):
        x_tr, y_tr = x.iloc[tr], y.iloc[tr]
        x_va, y_va = x.iloc[va], y.iloc[va]
        if y_tr.nunique() < 2 or len(x_va) == 0:
            continue
        m = clone(estimator)
        m.fit(x_tr, y_tr)
        pr = m.predict_proba(x_va)[:, 1]
        yt.append(y_va.to_numpy())
        yp.append((pr >= 0.5).astype(int))
        ypr.append(pr)
        idx.append(va)
    if not yt:
        return None, None, None, None
    return (
        np.concatenate(yt),
        np.concatenate(yp),
        np.concatenate(ypr),
        np.concatenate(idx),
    )


def wf_oos_reg(estimator, x, y, cv):
    yt, yp, idx = [], [], []
    for tr, va in purged_wf_indices(len(x), cv.n_splits, cv.purge):
        x_tr, y_tr = x.iloc[tr], y.iloc[tr]
        x_va, y_va = x.iloc[va], y.iloc[va]
        if len(x_va) == 0:
            continue
        m = clone(estimator)
        m.fit(x_tr, y_tr)
        pr = m.predict(x_va)
        yt.append(y_va.to_numpy())
        yp.append(pr)
        idx.append(va)
    if not yt:
        return None, None, None
    return np.concatenate(yt), np.concatenate(yp), np.concatenate(idx)


def select_gate(x_tr, y_tr, cv, tf_key):
    spw = _spw(y_tr)
    specs = gate_specs(spw)
    loaded = None
    if (
        _TUNED_GATE
        and tf_key in _TUNED_GATE
        and TRAIN_TARGET in _TUNED_GATE[tf_key]
        and "gate" in _TUNED_GATE[tf_key][TRAIN_TARGET]
    ):
        loaded = _TUNED_GATE[tf_key][TRAIN_TARGET]["gate"]
        print(
            f"  [GATE] spw={spw:.3f} | параметры из JSON "
            f"({TRAIN_TARGET}): {list(loaded)}"
        )
    else:
        print(
            f"  [GATE] spw={spw:.3f} | тюнинг на месте "
            f"(нет параметров для {tf_key}/{TRAIN_TARGET}/gate)"
        )
    table, tuned = [], {}
    for name, spec in specs.items():
        if loaded and name in loaded:
            params = dict(loaded[name].get("best_params", {}))
            est = clone(spec["estimator"]).set_params(**params)
        else:
            est = tune_candidate(spec, x_tr, y_tr, cv, scoring="f1")
        tuned[name] = est
        yt, yp, ypr, _ = wf_oos_clf(est, x_tr, y_tr, cv)
        if yt is None:
            print(f"    {name:5}: WF-OOS пуст")
            continue
        m = _binary_metrics(yt, yp, ypr)
        m["model"] = name
        table.append(m)
        print(
            f"    {name:5}: MCC={m['mcc']:.4f} "
            f"F1m={m['f1_macro']:.4f} AUC={m['auc']:.4f} "
            f"P+={m['prec_pos']:.3f} R+={m['rec_pos']:.3f}"
        )
    if not table:
        raise RuntimeError("GATE: ни один кандидат не оценён")

    def _key(r):
        v = r.get(GATE_SELECTION_METRIC, float("nan"))
        return -1e9 if v != v else v

    best = max(table, key=_key)
    bname = best["model"]
    print(
        f"  [GATE] лучшая по {GATE_SELECTION_METRIC.upper()}: "
        f"{bname} ({GATE_SELECTION_METRIC}={best[GATE_SELECTION_METRIC]:.4f})"
    )
    return bname, tuned[bname], pd.DataFrame(table)


def select_reg(
    x_tr,
    y_tr,
    cv,
    tf_key,
    huber_delta,
    reg_label="REG",
    params_key="dir_jump",
):
    """Автовыбор регрессора-на-скачках по MAE."""
    specs = reg_specs(huber_delta)
    loaded = None
    if (
        _TUNED_REG
        and tf_key in _TUNED_REG
        and TRAIN_TARGET in _TUNED_REG[tf_key]
        and params_key in _TUNED_REG[tf_key][TRAIN_TARGET]
    ):
        loaded = _TUNED_REG[tf_key][TRAIN_TARGET][params_key]
        print(
            f"  [{reg_label}] параметры из JSON "
            f"({TRAIN_TARGET}/{params_key}): {list(loaded)}"
        )
    else:
        print(
            f"  [{reg_label}] тюнинг на месте (neg_MAE) "
            f"(нет параметров для {tf_key}/{TRAIN_TARGET}/{params_key})"
        )
    table, tuned, oos_preds = [], {}, {}
    for name, spec in specs.items():
        if loaded and name in loaded:
            params = dict(loaded[name].get("best_params", {}))
            est = clone(spec["estimator"]).set_params(**params)
        else:
            est = tune_candidate(
                spec, x_tr, y_tr, cv, scoring="neg_mean_absolute_error"
            )
        tuned[name] = est
        yt, yp, idx = wf_oos_reg(est, x_tr, y_tr, cv)
        if yt is None:
            print(f"    {name:11}: WF-OOS пуст")
            continue
        m = _reg_metrics(yt, yp)
        m["model"] = name
        table.append(m)
        oos_preds[name] = pd.Series(yp, index=x_tr.index[idx], name=name)
        print(
            f"    {name:11}: MAE={m['mae']:.5f} "
            f"RMSE={m['rmse']:.5f} sign_acc={m['sign_acc']:.3f}"
        )
    if not table:
        raise RuntimeError(f"{reg_label}: ни один кандидат не оценён")
    if DIR_SELECTION_METRIC in ("mae", "rmse"):
        best = min(table, key=lambda r: r[DIR_SELECTION_METRIC])
    else:
        best = max(table, key=lambda r: r[DIR_SELECTION_METRIC])
    bname = best["model"]
    print(
        f"  [{reg_label}] лучшая по {DIR_SELECTION_METRIC.upper()}: "
        f"{bname} ({DIR_SELECTION_METRIC}={best[DIR_SELECTION_METRIC]:.5f})"
    )
    return bname, tuned[bname], pd.DataFrame(table), oos_preds[bname]


def tune_tau(gate_oos, reg_oos, y3_train, idx_common, label=""):
    """Подбор tau на |reg_pred| по максимуму MCC каскада (3 класса)."""
    g = gate_oos.loc[idx_common].to_numpy()
    r = reg_oos.loc[idx_common].to_numpy()
    y = y3_train.loc[idx_common].to_numpy()
    tau_max = float(np.quantile(np.abs(r), TAU_MAX_QUANTILE))
    tau_grid = np.linspace(0.0, tau_max, TAU_GRID_SIZE)
    rows = []
    for tau in tau_grid:
        casc = np.zeros_like(y, dtype=int)
        mv = (g == 1) & (np.abs(r) >= tau)
        casc[mv] = np.where(r[mv] > 0, 1, -1)
        mcc = (
            matthews_corrcoef(y, casc)
            if len(np.unique(y)) > 1
            else float("nan")
        )
        f1m = f1_score(
            y, casc, labels=[-1, 0, 1], average="macro", zero_division=0
        )
        active = float(np.mean(casc != 0))
        rows.append(
            {"tau": tau, "mcc": mcc, "f1_macro": f1m, "active_rate": active}
        )
    grid = pd.DataFrame(rows)
    best_row = grid.loc[grid["mcc"].idxmax()]
    print(
        f"  [TAU/{label}] best_tau={best_row['tau']:.6f} "
        f"-> MCC={best_row['mcc']:.4f} "
        f"F1m={best_row['f1_macro']:.4f} "
        f"active={best_row['active_rate']:.3f}"
    )
    return float(best_row["tau"]), grid, float(best_row["mcc"])


def report(title, y_true, y_pred, labels, names):
    print(f"  -- {title} --")
    print(
        classification_report(
            y_true,
            y_pred,
            labels=labels,
            target_names=names,
            digits=3,
            zero_division=0,
        )
    )
    print("  Confusion (строки=факт, столбцы=прогноз):")
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    print(
        pd.DataFrame(
            cm,
            index=[f"факт {n}" for n in names],
            columns=[f"пр.{n}" for n in names],
        )
    )


def eval_cascade(gate_pred, reg_pred, y3_te, tau, mode_label, index):
    casc = np.zeros(len(y3_te), dtype=int)
    mv = (gate_pred == 1) & (np.abs(reg_pred) >= tau)
    casc[mv] = np.where(reg_pred[mv] > 0, 1, -1)
    casc_s = pd.Series(casc, index=index)
    acc = accuracy_score(y3_te, casc)
    f1m = f1_score(
        y3_te, casc, labels=[-1, 0, 1], average="macro", zero_division=0
    )
    mcc = (
        matthews_corrcoef(y3_te, casc)
        if y3_te.nunique() > 1
        else float("nan")
    )
    active = float((casc != 0).mean())
    print(
        f"  [КАСКАД-{mode_label} / HOLD-OUT] "
        f"ACC={acc:.3f} F1m={f1m:.3f} MCC={mcc:.4f} | "
        f"active={active:.3f} (tau={tau:.5f})"
    )
    report(
        f"КАСКАД-{mode_label} (-1=DOWN, 0=FLAT, +1=UP)",
        y3_te,
        casc_s,
        [-1, 0, 1],
        ["DOWN", "FLAT", "UP"],
    )
    return {
        "acc": acc,
        "f1_macro": f1m,
        "mcc": mcc,
        "active_rate": active,
        "tau": tau,
    }


def run_timeframe(
    name,
    path_all,
    path_jump,
    vol_indicator,
    dynamic_k,
    static_thresh,
    purge,
    huber_delta,
):
    print(f"\n  ТАЙМФРЕЙМ {name.upper()} | huber_delta={huber_delta}")
    print(f"  all={path_all} | jump={path_jump}")

    df_all = pd.read_csv(path_all, index_col=0, parse_dates=True)
    df_all, vol_shift = build_targets(
        df_all, vol_indicator, dynamic_k, static_thresh
    )
    feat_cols = split_features(df_all)
    print(f"  [ALL]  строк={len(df_all)} | признаков={len(feat_cols)}")
    for t in ["t_static", "t_dynamic"]:
        vc = df_all[t].value_counts().sort_index().to_dict()
        sig = (df_all[t] != 0).mean()
        print(f"  {t:11}: {vc} | signal_rate={sig:.3f}")
    print(
        f"  fwd_ret: mean={df_all[FWD_COL].mean():.5f} "
        f"std={df_all[FWD_COL].std():.5f} "
        f"q01={df_all[FWD_COL].quantile(0.01):.4f} "
        f"q99={df_all[FWD_COL].quantile(0.99):.4f}"
    )

    df_jump = pd.read_csv(path_jump, index_col=0, parse_dates=True)
    df_jump, _ = build_targets(
        df_jump, vol_indicator, dynamic_k, static_thresh
    )
    feat_cols_jump = split_features_jump(df_jump)
    present_price_ctx = [
        c for c in _PRICE_CONTEXT_COLS if c in df_jump.columns
    ]
    feat_cols_jump = list(
        dict.fromkeys(feat_cols_jump + present_price_ctx)
    )
    print(
        f"  [JUMP] строк={len(df_jump)} | "
        f"признаков={len(feat_cols_jump)} "
        f"(в т.ч. price_ctx={len(present_price_ctx)})"
    )
    vc_jump = df_jump["t_static"].value_counts().sort_index().to_dict()
    print(f"  [JUMP] t_static: {vc_jump}")

    cv = PurgedWalkForward(N_WF_SPLITS, purge)

    n_all = len(df_all)
    cut = int(n_all * (1.0 - TEST_RATIO))
    df_tr = df_all.iloc[:cut]
    df_te = df_all.iloc[cut:]
    train_end = df_tr.index[-1]
    df_jump_tr = df_jump.loc[df_jump.index <= train_end]
    df_jump_te = df_jump.loc[df_jump.index > train_end]
    print(f"  all:  train={len(df_tr)} test={len(df_te)}")
    print(f"  jump: train={len(df_jump_tr)} test={len(df_jump_te)}")
    print(
        f"  train t_static : "
        f"{df_tr['t_static'].value_counts().sort_index().to_dict()}"
    )
    print(
        f"  train t_dynamic: "
        f"{df_tr['t_dynamic'].value_counts().sort_index().to_dict()}"
    )

    print(f"  GATE (train target={TRAIN_TARGET}, все бары)")
    xg_tr, yg_tr = make_gate_xy(df_tr, TRAIN_TARGET, feat_cols)
    g_name, g_model, g_tbl = select_gate(xg_tr, yg_tr, cv, tf_key=name)
    _, yp_g, _, idx_g = wf_oos_clf(g_model, xg_tr, yg_tr, cv)
    gate_oos = pd.Series(yp_g, index=xg_tr.index[idx_g], name="gate")

    print("  REG (только скачки, y=fwd_ret + price_context)")
    xrj_tr, yrj_tr = make_reg_jump_xy(df_jump_tr, feat_cols_jump)
    r_name, r_model, r_tbl, r_oos = select_reg(
        xrj_tr,
        yrj_tr,
        cv,
        tf_key=name,
        huber_delta=huber_delta,
        reg_label="REG",
        params_key=REG_PARAMS_KEY,
    )

    print("  ПОДБОР TAU (WF-OOS train)")
    taus = {}
    tau_mccs = {}
    for mode in EVAL_MODES:
        y3_tr = df_tr[f"t_{mode}"].astype(int)
        common = (
            gate_oos.index.intersection(r_oos.index).intersection(
                df_tr.index
            )
        )
        if len(common) < 50:
            raise RuntimeError(
                f"[{name}/{mode}] Мало общих OOS-точек gate∩reg: "
                f"{len(common)} (нужно >=50)."
            )
        tau_v, _grid, mcc_v = tune_tau(
            gate_oos, r_oos, y3_tr, common, label=mode
        )
        taus[mode] = tau_v
        tau_mccs[mode] = mcc_v

    g_final = clone(g_model).fit(xg_tr, yg_tr)
    r_final = clone(r_model).fit(xrj_tr, yrj_tr)

    print("  HOLD-OUT: gate + reg x 2 режима tau")
    xte_all = df_te[feat_cols].copy()
    ok_te = xte_all.notna().all(axis=1)
    xte_all = xte_all.loc[ok_te]

    gate_pred = g_final.predict(xte_all)
    gate_proba = g_final.predict_proba(xte_all)[:, 1]
    gate_true_s = (df_te.loc[ok_te, "t_static"] != 0).astype(int)
    m_gate = _binary_metrics(gate_true_s, gate_pred, gate_proba)
    print(
        f"  [GATE / HOLD-OUT vs t_static] модель={g_name} | "
        f"MCC={m_gate['mcc']:.4f} F1m={m_gate['f1_macro']:.4f} "
        f"AUC={m_gate['auc']:.4f} "
        f"P(move)={m_gate['prec_pos']:.3f} "
        f"R(move)={m_gate['rec_pos']:.3f}"
    )

    xte_jump_raw = df_jump_te[feat_cols_jump].copy()
    ok_te_jump = xte_jump_raw.notna().all(axis=1)
    xte_jump = xte_jump_raw.loc[ok_te_jump]
    reg_pred_sparse = (
        r_final.predict(xte_jump) if len(xte_jump) > 0 else np.array([])
    )

    reg_pred_full = np.zeros(len(xte_all))
    jump_mask_te = xte_all.index.isin(xte_jump.index)
    if jump_mask_te.any():
        pos_in_all = np.where(jump_mask_te)[0]
        common_idx = xte_all.index[jump_mask_te]
        pos_in_jump = [xte_jump.index.get_loc(i) for i in common_idx]
        reg_pred_full[pos_in_all] = reg_pred_sparse[pos_in_jump]

    if len(xte_jump) > 0:
        yfwd_te_jump = df_jump_te.loc[xte_jump.index, FWD_COL].to_numpy()
        m_reg = _reg_metrics(yfwd_te_jump, reg_pred_sparse)
        for mode in EVAL_MODES:
            mv = (
                df_jump_te.loc[xte_jump.index, f"t_{mode}"] != 0
            ).to_numpy()
            if mv.sum() > 0:
                m_reg[f"sign_acc_jumps_{mode}"] = float(
                    np.mean(
                        np.sign(reg_pred_sparse[mv])
                        == np.sign(yfwd_te_jump[mv])
                    )
                )
            else:
                m_reg[f"sign_acc_jumps_{mode}"] = float("nan")
    else:
        m_reg = _reg_metrics(np.array([]), np.array([]))
        for mode in EVAL_MODES:
            m_reg[f"sign_acc_jumps_{mode}"] = float("nan")
    print(
        f"  [REG / HOLD-OUT, на скачках] "
        f"n={m_reg['n']} MAE={m_reg['mae']:.5f} RMSE={m_reg['rmse']:.5f} | "
        f"sign_acc={m_reg['sign_acc']:.3f} "
        f"signJ_stat={m_reg.get('sign_acc_jumps_static', float('nan')):.3f} "
        f"signJ_dyn={m_reg.get('sign_acc_jumps_dynamic', float('nan')):.3f}"
    )

    casc_results = {}
    for mode in EVAL_MODES:
        y3_te = df_te.loc[ok_te, f"t_{mode}"].astype(int)
        casc_results[mode] = eval_cascade(
            gate_pred,
            reg_pred_full,
            y3_te,
            taus[mode],
            mode_label=mode,
            index=xte_all.index,
        )

    print("  DEPLOY: переобучение на всей истории")
    xg_all, yg_all = make_gate_xy(df_all, TRAIN_TARGET, feat_cols)
    xrj_all, yrj_all = make_reg_jump_xy(df_jump, feat_cols_jump)
    g_deploy = clone(g_model).fit(xg_all, yg_all)
    r_deploy = clone(r_model).fit(xrj_all, yrj_all)
    print(f"  g_deploy: {g_name} (обучен на {len(xg_all)} барах)")
    print(f"  r_deploy: {r_name} (обучен на {len(xrj_all)} скачках)")

    return {
        "name": name,
        "train_target": TRAIN_TARGET,
        "g_deploy": g_deploy,
        "r_deploy": r_deploy,
        "g_final": g_final,
        "r_final": r_final,
        "gate_model_name": g_name,
        "dir_model_name": r_name,
        "gate_select_table": g_tbl,
        "reg_select_table": r_tbl,
        "taus": taus,
        "tau_mccs": tau_mccs,
        "m_gate": m_gate,
        "m_reg": m_reg,
        "cascade": casc_results,
        "feat_cols": feat_cols,
        "feat_cols_jump": feat_cols_jump,
        "X_full": df_all[feat_cols].copy(),
        "X_jump_full": df_jump[feat_cols_jump].copy(),
        "price_full": (
            df_all["c_close"].copy()
            if "c_close" in df_all.columns
            else None
        ),
        "vol_indicator": (
            vol_indicator
            if vol_indicator in df_all.columns
            else VOL_FALLBACK
        ),
        "vol_shift_full": vol_shift,
        "dynamic_k": dynamic_k,
        "static_thresh": static_thresh,
        "last_ts": df_all.index[-1],
    }


def main():
    global _TUNED_GATE, _TUNED_REG

    print("ОБУЧЕНИЕ v9 — GATE + REG (только скачки) | один каскад")
    _TUNED_GATE = _load_tuned(TUNED_PARAMS_FILE_GATE, "GATE params")
    _TUNED_REG = _load_tuned(TUNED_PARAMS_FILE_REG, "REG params")

    _resolve_path(DATA_1H, "1H-all")
    _resolve_path(DATA_6H, "6H-all")
    _resolve_path(DATA_1H_JUMP, "1H-jump")
    _resolve_path(DATA_6H_JUMP, "6H-jump")

    results = {
        "1h": run_timeframe(
            name="1h",
            path_all=DATA_1H,
            path_jump=DATA_1H_JUMP,
            vol_indicator=VOL_INDICATOR_1H,
            dynamic_k=DYNAMIC_K_1H,
            static_thresh=STATIC_THRESH_1H,
            purge=PURGE_1H,
            huber_delta=HUBER_DELTA_1H,
        ),
        "6h": run_timeframe(
            name="6h",
            path_all=DATA_6H,
            path_jump=DATA_6H_JUMP,
            vol_indicator=VOL_INDICATOR_6H,
            dynamic_k=DYNAMIC_K_6H,
            static_thresh=STATIC_THRESH_6H,
            purge=PURGE_6H,
            huber_delta=HUBER_DELTA_6H,
        ),
    }

    print("ИТОГОВАЯ СВОДКА — 2 ТФ x 1 каскад x 2 режима")
    print(
        f"  {'TF':<4}{'gate':<7}{'reg':<13}{'mode':<9}"
        f"{'tau':>9}{'MCC':>10}{'F1':>9}{'active':>9}{'signJ':>8}"
    )
    for tf, res in results.items():
        for mode in EVAL_MODES:
            c = res["cascade"][mode]
            sj = res["m_reg"].get(f"sign_acc_jumps_{mode}", float("nan"))
            print(
                f"  {tf.upper():<4}"
                f"{res['gate_model_name']:<7}"
                f"{res['dir_model_name']:<13}"
                f"{mode:<9}"
                f"{c['tau']:>9.5f}"
                f"{c['mcc']:>10.4f}"
                f"{c['f1_macro']:>9.3f}"
                f"{c['active_rate']:>9.3f}"
                f"{sj:>8.3f}"
            )

    bundle = {}
    for tf, res in results.items():
        bundle[tf] = {
            "name": res["name"],
            "train_target": res["train_target"],
            "gate_model_name": res["gate_model_name"],
            "dir_model_name": res["dir_model_name"],
            "g_deploy": res["g_deploy"],
            "r_deploy": res["r_deploy"],
            "taus": res["taus"],
            "tau_mccs": res["tau_mccs"],
            "m_gate": res["m_gate"],
            "m_reg": res["m_reg"],
            "cascade": res["cascade"],
            "feat_cols": res["feat_cols"],
            "feat_cols_jump": res["feat_cols_jump"],
            "X_full": res["X_full"],
            "X_jump_full": res["X_jump_full"],
            "price_full": res["price_full"],
            "vol_indicator": res["vol_indicator"],
            "vol_shift_full": res["vol_shift_full"],
            "dynamic_k": res["dynamic_k"],
            "static_thresh": res["static_thresh"],
            "last_ts": res["last_ts"],
        }

    bundle_file = "cascade_bundle_v9.pkl"
    with open(bundle_file, "wb") as f:
        pickle.dump(bundle, f, protocol=pickle.HIGHEST_PROTOCOL)
    sz = os.path.getsize(bundle_file) / 1024
    print(f"  Бандл -> {bundle_file} ({sz:.0f} КБ)")
    print(f"     ТФ: {list(bundle)} | режимы tau: {EVAL_MODES}")

def run_training(features_dir=None, models_dir=None) -> dict:
    """Run v9 training and return per-timeframe results (no pickle save)."""
    global DATA_1H, DATA_6H, DATA_1H_JUMP, DATA_6H_JUMP
    global TUNED_PARAMS_FILE_GATE, TUNED_PARAMS_FILE_REG, _TUNED_GATE, _TUNED_REG

    if features_dir is not None:
        features_dir = str(features_dir)
        DATA_1H = f"{features_dir}/final_dataset_1h.csv"
        DATA_6H = f"{features_dir}/final_dataset_6h.csv"
        DATA_1H_JUMP = f"{features_dir}/final_dataset_1h_jump.csv"
        DATA_6H_JUMP = f"{features_dir}/final_dataset_6h_jump.csv"
    if models_dir is not None:
        models_dir = str(models_dir)
        TUNED_PARAMS_FILE_GATE = f"{models_dir}/best_params.json"
        TUNED_PARAMS_FILE_REG = f"{models_dir}/best_params_reg.json"

    _TUNED_GATE = _load_tuned(TUNED_PARAMS_FILE_GATE, "GATE params")
    _TUNED_REG = _load_tuned(TUNED_PARAMS_FILE_REG, "REG params")

    for path, label in [
        (DATA_1H, "1H-all"),
        (DATA_6H, "6H-all"),
        (DATA_1H_JUMP, "1H-jump"),
        (DATA_6H_JUMP, "6H-jump"),
    ]:
        _resolve_path(path, label)

    return {
        "1h": run_timeframe(
            name="1h",
            path_all=DATA_1H,
            path_jump=DATA_1H_JUMP,
            vol_indicator=VOL_INDICATOR_1H,
            dynamic_k=DYNAMIC_K_1H,
            static_thresh=STATIC_THRESH_1H,
            purge=PURGE_1H,
            huber_delta=HUBER_DELTA_1H,
        ),
        "6h": run_timeframe(
            name="6h",
            path_all=DATA_6H,
            path_jump=DATA_6H_JUMP,
            vol_indicator=VOL_INDICATOR_6H,
            dynamic_k=DYNAMIC_K_6H,
            static_thresh=STATIC_THRESH_6H,
            purge=PURGE_6H,
            huber_delta=HUBER_DELTA_6H,
        ),
    }


if __name__ == "__main__":
    main()