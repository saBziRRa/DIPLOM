"""Блок тюнинга v9 — гиперпараметры gate + reg_jump (Optuna).

Парный с training_block_v9.py. REG-ALL убран: тюнятся только gate
(AUC, maximize) на всех барах и регрессор-на-скачках (MAE, minimize)
на jump-датасете. CV — purged walk-forward с embargo.

Вывод по двум файлам, как ожидает training_block_v9:
    best_params.json      -> results[tf][t_static]['gate'][model]
    best_params_reg.json  -> results[tf][t_static]['dir_jump'][model]
"""

import json
import os
import time
import warnings
from datetime import datetime
from typing import Any, Callable

import numpy as np
import optuna
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import ElasticNet
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier, XGBRegressor

try:
    from lightgbm import LGBMClassifier, LGBMRegressor
    _HAS_LGBM = True
except ImportError:
    _HAS_LGBM = False

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.WARNING)

BUDGET: str = "standart"
BUDGET_CFG: dict[str, dict[str, int]] = {
    "light": {"n_trials": 40, "timeout_min": 8},
    "standart": {"n_trials": 80, "timeout_min": 18},
    "heavy": {"n_trials": 150, "timeout_min": 35},
}

MODELS_TO_TUNE_GATE: list[str] | None = None
MODELS_TO_TUNE_REG: list[str] | None = None

ALL_GATE_MODELS: list[str] = ["xgb", "lgbm"]
ALL_REG_MODELS: list[str] = [
    "xgb_huber",
    "lgbm_huber",
    "hgb_mae",
    "elasticnet",
]

GATE_OBJECTIVE: str = "auc"
DIR_OBJECTIVE: str = "mae"

TRAIN_TARGET: str = "t_static"

DATA_1H: str = "final_dataset_1h.csv"
DATA_6H: str = "final_dataset_6h.csv"
DATA_1H_JUMP: str = "final_dataset_1h_jump.csv"
DATA_6H_JUMP: str = "final_dataset_6h_jump.csv"

STATIC_THRESH_1H: float = 0.008
STATIC_THRESH_6H: float = 0.015

N_WF_SPLITS: int = 5
EMBARGO_1H: int = 1
EMBARGO_6H: int = 6

GATE_OUTPUT_FILE: str = "best_params.json"
REG_OUTPUT_FILE: str = "best_params_reg.json"

RANDOM_STATE: int = 42
FWD_COL: str = "fwd_ret"

_LEAK_PREFIX: tuple[str, ...] = (
    "fwd_ret",
    "target",
    "t_static",
    "t_dynamic",
)
_NON_FEATURE: set[str] = {"c_close"}
_PRICE_CONTEXT_COLS: set[str] = {
    "price_log",
    "price_rank",
    "price_vs_sma_ratio",
    "price_zscore",
}


def _resolve_path(path: str, label: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"[{label}] не найден '{path}'")
    print(f"  [{label}] вход: {path}")
    return path


def build_data(df: pd.DataFrame, static_thresh: float) -> pd.DataFrame:
    """Добавляет t_static (gate-таргет и отчётность)."""
    if FWD_COL not in df.columns:
        raise KeyError(f"Нет колонки '{FWD_COL}'")
    df = df.dropna(subset=[FWD_COL]).copy()
    fwd = df[FWD_COL]
    df["t_static"] = np.where(
        fwd > static_thresh, 1, np.where(fwd < -static_thresh, -1, 0)
    )
    return df


def split_features(df: pd.DataFrame) -> list[str]:
    """Признаки для gate (без leakage и мета-колонок)."""
    return [
        c
        for c in df.columns
        if c not in _NON_FEATURE and not c.startswith(_LEAK_PREFIX)
    ]


def split_features_jump(df: pd.DataFrame) -> list[str]:
    """Признаки для регрессора (price_* уже в датасете от Блока 3)."""
    return [
        c
        for c in df.columns
        if c not in _NON_FEATURE and not c.startswith(_LEAK_PREFIX)
    ]


def make_gate_xy(
    df: pd.DataFrame, feat_cols: list[str]
) -> tuple[pd.DataFrame, pd.Series]:
    """Gate: все бары, y=1 если |fwd_ret| > static_thresh."""
    x = df[feat_cols].copy()
    y = (df["t_static"] != 0).astype(int)
    ok = x.notna().all(axis=1)
    return x.loc[ok], y.loc[ok]


def make_reg_jump_xy(
    df_jump: pd.DataFrame, feat_cols_jump: list[str]
) -> tuple[pd.DataFrame, pd.Series]:
    """REG: только строки скачков, y=fwd_ret, признаки с price_*."""
    x = df_jump[feat_cols_jump].copy()
    y = df_jump[FWD_COL].copy()
    ok = x.notna().all(axis=1) & y.notna()
    return x.loc[ok], y.loc[ok]


def purged_embargo_splits(
    n: int, n_splits: int, embargo: int
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Список (train_idx, val_idx) с purge-gap = embargo."""
    fold = n // (n_splits + 1)
    out: list[tuple[np.ndarray, np.ndarray]] = []
    for k in range(1, n_splits + 1):
        cut = fold * k
        tr_end = cut - embargo
        va_end = min(cut + fold, n)
        if tr_end <= 50 or va_end <= cut + 10:
            continue
        out.append((np.arange(0, tr_end), np.arange(cut, va_end)))
    return out


def _reg_metrics(
    y_true: np.ndarray, y_pred: np.ndarray
) -> dict[str, float]:
    """MAE / RMSE / sign_acc на ненулевых барах."""
    mae = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mask = np.abs(y_true) > 1e-9
    sign_acc = (
        float(np.mean(np.sign(y_pred[mask]) == np.sign(y_true[mask])))
        if mask.sum() > 0
        else float("nan")
    )
    return {"mae": mae, "rmse": rmse, "sign_acc": sign_acc}


def _clf_metrics(
    y_true: np.ndarray, y_proba: np.ndarray
) -> dict[str, float]:
    """AUC для бинарного gate-классификатора."""
    if len(np.unique(y_true)) < 2:
        return {"auc": float("nan")}
    try:
        auc = roc_auc_score(y_true, y_proba)
    except ValueError:
        auc = float("nan")
    return {"auc": auc}


def cv_score_reg(
    estimator_fn: Callable,
    x: pd.DataFrame,
    y: pd.Series,
    splits: list[tuple[np.ndarray, np.ndarray]],
    target_metric: str,
) -> tuple[dict[str, float], float]:
    """WF-OOS CV для регрессора. Возвращает (avg_metrics, score)."""
    accum: dict[str, list[float]] = {"mae": [], "rmse": [], "sign_acc": []}
    n_x = len(x)
    for tr, va in splits:
        if tr.max() >= n_x or va.max() >= n_x or len(va) == 0:
            raise ValueError(
                f"Сплит вне x: n_x={n_x}, "
                f"tr.max={tr.max()}, va.max={va.max()}"
            )
        m = estimator_fn()
        m.fit(x.iloc[tr], y.iloc[tr])
        pr = m.predict(x.iloc[va])
        d = _reg_metrics(y.iloc[va].to_numpy(), np.asarray(pr))
        for k in accum:
            v = d[k]
            if v == v:
                accum[k].append(v)
    avg = {
        k: (float(np.mean(v)) if v else float("nan"))
        for k, v in accum.items()
    }
    return avg, avg.get(target_metric, float("nan"))


def cv_score_clf(
    estimator_fn: Callable,
    x: pd.DataFrame,
    y: pd.Series,
    splits: list[tuple[np.ndarray, np.ndarray]],
    target_metric: str,
) -> tuple[dict[str, float], float]:
    """WF-OOS CV для классификатора. Возвращает (avg_metrics, score)."""
    accum: dict[str, list[float]] = {"auc": []}
    n_x = len(x)
    for tr, va in splits:
        if tr.max() >= n_x or va.max() >= n_x or len(va) == 0:
            raise ValueError(
                f"Сплит вне x: n_x={n_x}, "
                f"tr.max={tr.max()}, va.max={va.max()}"
            )
        ytr = y.iloc[tr]
        if ytr.nunique() < 2:
            continue
        m = estimator_fn()
        m.fit(x.iloc[tr], ytr)
        pr = m.predict_proba(x.iloc[va])[:, 1]
        d = _clf_metrics(y.iloc[va].to_numpy(), np.asarray(pr))
        for k in accum:
            v = d[k]
            if v == v:
                accum[k].append(v)
    avg = {
        k: (float(np.mean(v)) if v else float("nan"))
        for k, v in accum.items()
    }
    return avg, avg.get(target_metric, float("nan"))


def suggest_gate_xgb(trial: optuna.Trial, spw: float) -> dict[str, Any]:
    return {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "tree_method": "hist",
        "n_jobs": -1,
        "verbosity": 0,
        "random_state": RANDOM_STATE,
        "scale_pos_weight": spw,
        "n_estimators": trial.suggest_int("n_estimators", 100, 700),
        "max_depth": trial.suggest_int("max_depth", 3, 9),
        "learning_rate": trial.suggest_float(
            "learning_rate", 0.005, 0.15, log=True
        ),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float(
            "colsample_bytree", 0.5, 1.0
        ),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
        "gamma": trial.suggest_float("gamma", 0.0, 5.0),
        "reg_alpha": trial.suggest_float(
            "reg_alpha", 1e-4, 10.0, log=True
        ),
        "reg_lambda": trial.suggest_float(
            "reg_lambda", 1e-4, 10.0, log=True
        ),
    }


def suggest_gate_lgbm(trial: optuna.Trial) -> dict[str, Any]:
    return {
        "objective": "binary",
        "is_unbalance": True,
        "n_jobs": -1,
        "verbose": -1,
        "random_state": RANDOM_STATE,
        "n_estimators": trial.suggest_int("n_estimators", 100, 700),
        "num_leaves": trial.suggest_int("num_leaves", 8, 128),
        "max_depth": trial.suggest_int("max_depth", -1, 12),
        "learning_rate": trial.suggest_float(
            "learning_rate", 0.005, 0.15, log=True
        ),
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 5, 100),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "subsample_freq": trial.suggest_int("subsample_freq", 1, 10),
        "colsample_bytree": trial.suggest_float(
            "colsample_bytree", 0.5, 1.0
        ),
        "reg_alpha": trial.suggest_float(
            "reg_alpha", 1e-4, 10.0, log=True
        ),
        "reg_lambda": trial.suggest_float(
            "reg_lambda", 1e-4, 10.0, log=True
        ),
    }


def suggest_xgb_huber(trial: optuna.Trial) -> dict[str, Any]:
    return {
        "objective": "reg:pseudohubererror",
        "tree_method": "hist",
        "n_jobs": -1,
        "verbosity": 0,
        "random_state": RANDOM_STATE,
        "huber_slope": trial.suggest_float(
            "huber_slope", 1e-3, 5e-2, log=True
        ),
        "n_estimators": trial.suggest_int("n_estimators", 150, 800),
        "max_depth": trial.suggest_int("max_depth", 3, 9),
        "learning_rate": trial.suggest_float(
            "learning_rate", 0.005, 0.15, log=True
        ),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float(
            "colsample_bytree", 0.5, 1.0
        ),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 30),
        "gamma": trial.suggest_float("gamma", 0.0, 5.0),
        "reg_alpha": trial.suggest_float(
            "reg_alpha", 1e-4, 10.0, log=True
        ),
        "reg_lambda": trial.suggest_float(
            "reg_lambda", 1e-4, 10.0, log=True
        ),
    }


def suggest_lgbm_huber(trial: optuna.Trial) -> dict[str, Any]:
    return {
        "objective": "huber",
        "n_jobs": -1,
        "verbose": -1,
        "random_state": RANDOM_STATE,
        "alpha": trial.suggest_float("alpha", 1e-3, 5e-2, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 150, 800),
        "num_leaves": trial.suggest_int("num_leaves", 8, 128),
        "max_depth": trial.suggest_int("max_depth", -1, 12),
        "learning_rate": trial.suggest_float(
            "learning_rate", 0.005, 0.15, log=True
        ),
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 5, 100),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "subsample_freq": trial.suggest_int("subsample_freq", 1, 10),
        "colsample_bytree": trial.suggest_float(
            "colsample_bytree", 0.5, 1.0
        ),
        "reg_alpha": trial.suggest_float(
            "reg_alpha", 1e-4, 10.0, log=True
        ),
        "reg_lambda": trial.suggest_float(
            "reg_lambda", 1e-4, 10.0, log=True
        ),
        "min_gain_to_split": trial.suggest_float(
            "min_gain_to_split", 0.0, 1.0
        ),
    }


def suggest_hgb_mae(trial: optuna.Trial) -> dict[str, Any]:
    return {
        "loss": "absolute_error",
        "random_state": RANDOM_STATE,
        "max_iter": trial.suggest_int("max_iter", 150, 800),
        "max_depth": trial.suggest_categorical(
            "max_depth", [3, 4, 5, 6, 8, 10, None]
        ),
        "learning_rate": trial.suggest_float(
            "learning_rate", 0.005, 0.15, log=True
        ),
        "min_samples_leaf": trial.suggest_int(
            "min_samples_leaf", 10, 150
        ),
        "l2_regularization": trial.suggest_float(
            "l2_regularization", 1e-4, 10.0, log=True
        ),
        "max_leaf_nodes": trial.suggest_int("max_leaf_nodes", 15, 127),
    }


def suggest_elasticnet(trial: optuna.Trial) -> dict[str, Any]:
    return {
        "model__alpha": trial.suggest_float(
            "model__alpha", 1e-5, 1.0, log=True
        ),
        "model__l1_ratio": trial.suggest_float(
            "model__l1_ratio", 0.05, 0.95
        ),
        "model__max_iter": 5000,
        "model__random_state": RANDOM_STATE,
    }


def make_gate_estimator(model_name: str, params: dict[str, Any]) -> Any:
    """Создаёт gate-классификатор по имени и параметрам."""
    if model_name == "xgb":
        return XGBClassifier(**params)
    if model_name == "lgbm":
        if not _HAS_LGBM:
            raise RuntimeError("lightgbm не установлен")
        return LGBMClassifier(**params)
    raise ValueError(f"Неизвестная gate-модель: '{model_name}'")


def make_reg_estimator(model_name: str, params: dict[str, Any]) -> Any:
    """Создаёт регрессор по имени и параметрам."""
    if model_name == "xgb_huber":
        return XGBRegressor(**params)
    if model_name == "lgbm_huber":
        if not _HAS_LGBM:
            raise RuntimeError("lightgbm не установлен")
        return LGBMRegressor(**params)
    if model_name == "hgb_mae":
        return HistGradientBoostingRegressor(**params)
    if model_name == "elasticnet":
        pipe = Pipeline(
            [("scaler", StandardScaler()), ("model", ElasticNet())]
        )
        return pipe.set_params(**params)
    raise ValueError(f"Неизвестная reg-модель: '{model_name}'")


_REG_SUGGEST_FN: dict[str, Callable] = {
    "xgb_huber": suggest_xgb_huber,
    "lgbm_huber": suggest_lgbm_huber,
    "hgb_mae": suggest_hgb_mae,
    "elasticnet": suggest_elasticnet,
}


def _run_study(
    objective_fn: Callable[[optuna.Trial], float],
    direction: str,
    n_trials: int,
    timeout_sec: float,
    study_name: str,
) -> optuna.Study:
    """Создаёт и запускает Optuna study."""
    sampler = optuna.samplers.TPESampler(
        seed=RANDOM_STATE, n_startup_trials=10
    )
    pruner = optuna.pruners.MedianPruner(n_startup_trials=10)
    study = optuna.create_study(
        direction=direction,
        sampler=sampler,
        pruner=pruner,
        study_name=study_name,
    )
    study.optimize(
        objective_fn,
        n_trials=n_trials,
        timeout=timeout_sec,
        show_progress_bar=False,
    )
    return study


def _study_result(
    study: optuna.Study,
    model_name: str,
    target_metric: str,
    elapsed: float,
) -> dict[str, Any]:
    """Упаковывает результат study в стандартный словарь."""
    completed = [
        t
        for t in study.trials
        if t.state == optuna.trial.TrialState.COMPLETE
    ]
    if not completed:
        return {
            "model": model_name,
            "target_metric": target_metric,
            "best_score": float("nan"),
            "best_params": {},
            "all_metrics_at_best": {},
            "n_trials": len(study.trials),
            "elapsed_sec": round(elapsed, 1),
            "warning": "no completed trials",
        }
    best = study.best_trial
    return {
        "model": model_name,
        "target_metric": target_metric,
        "best_score": float(best.value),
        "best_params": dict(best.params),
        "all_metrics_at_best": dict(best.user_attrs),
        "n_trials": len(study.trials),
        "elapsed_sec": round(elapsed, 1),
    }


def tune_gate_one(
    model_name: str,
    x: pd.DataFrame,
    y: pd.Series,
    splits: list[tuple[np.ndarray, np.ndarray]],
    n_trials: int,
    timeout_sec: float,
    study_name: str,
) -> dict[str, Any]:
    """Optuna study для одного gate-классификатора (AUC, maximize)."""
    spw = float((y == 0).sum()) / max(float((y == 1).sum()), 1.0)

    def objective(trial: optuna.Trial) -> float:
        if model_name == "xgb":
            params = suggest_gate_xgb(trial, spw)
        elif model_name == "lgbm":
            params = suggest_gate_lgbm(trial)
        else:
            raise optuna.TrialPruned(
                f"Неизвестная gate-модель: {model_name}"
            )
        try:
            avg, score = cv_score_clf(
                lambda: make_gate_estimator(model_name, params),
                x,
                y,
                splits,
                GATE_OBJECTIVE,
            )
        except Exception as exc:
            raise optuna.TrialPruned(f"fit failed: {exc}")
        for k, v in avg.items():
            trial.set_user_attr(k, v)
        return score if score == score else -1.0

    t0 = time.time()
    study = _run_study(
        objective, "maximize", n_trials, timeout_sec, study_name
    )
    return _study_result(
        study, model_name, GATE_OBJECTIVE, time.time() - t0
    )


def tune_reg_one(
    model_name: str,
    x: pd.DataFrame,
    y: pd.Series,
    splits: list[tuple[np.ndarray, np.ndarray]],
    n_trials: int,
    timeout_sec: float,
    study_name: str,
) -> dict[str, Any]:
    """Optuna study для регрессора-на-скачках (MAE, minimize)."""
    suggest = _REG_SUGGEST_FN[model_name]

    def objective(trial: optuna.Trial) -> float:
        params = suggest(trial)
        try:
            avg, score = cv_score_reg(
                lambda: make_reg_estimator(model_name, params),
                x,
                y,
                splits,
                DIR_OBJECTIVE,
            )
        except Exception as exc:
            raise optuna.TrialPruned(f"fit failed: {exc}")
        for k, v in avg.items():
            trial.set_user_attr(k, v)
        return score if score == score else 1e9

    t0 = time.time()
    study = _run_study(
        objective, "minimize", n_trials, timeout_sec, study_name
    )
    return _study_result(
        study, model_name, DIR_OBJECTIVE, time.time() - t0
    )


def tune_timeframe(
    name: str,
    path_all: str,
    path_jump: str,
    static_thresh: float,
    embargo: int,
    budget: dict[str, int],
) -> dict[str, Any]:
    """Полный цикл тюнинга для одного ТФ: gate + dir_jump."""
    print(
        f"  ТАЙМФРЕЙМ {name.upper()} — БЮДЖЕТ '{BUDGET}' | "
        f"embargo={embargo}"
    )
    n_trials = budget["n_trials"]
    timeout_sec = budget["timeout_min"] * 60

    df_all = pd.read_csv(path_all, index_col=0, parse_dates=True)
    df_all = build_data(df_all, static_thresh)
    feat_cols = split_features(df_all)
    print(f"  [ALL]  строк={len(df_all)} | признаков={len(feat_cols)}")
    print(
        f"  fwd_ret: mean={df_all[FWD_COL].mean():.5f} "
        f"std={df_all[FWD_COL].std():.5f} "
        f"q01={df_all[FWD_COL].quantile(0.01):.4f} "
        f"q99={df_all[FWD_COL].quantile(0.99):.4f}"
    )
    vc_all = df_all["t_static"].value_counts().sort_index().to_dict()
    print(f"  t_static: {vc_all}")

    xg, yg = make_gate_xy(df_all, feat_cols)
    splits_gate = purged_embargo_splits(len(xg), N_WF_SPLITS, embargo)
    if not splits_gate:
        raise RuntimeError(
            f"[{name}/gate] Не построились сплиты на {len(xg)} строк"
        )

    df_jump = pd.read_csv(path_jump, index_col=0, parse_dates=True)
    df_jump = build_data(df_jump, static_thresh)
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
        f"(price_ctx={len(present_price_ctx)})"
    )
    vc_jump = df_jump["t_static"].value_counts().sort_index().to_dict()
    print(f"  [JUMP] t_static: {vc_jump}")

    xrj, yrj = make_reg_jump_xy(df_jump, feat_cols_jump)
    splits_jump = purged_embargo_splits(len(xrj), N_WF_SPLITS, embargo)
    if not splits_jump:
        print(
            f"  warn [{name}/jump] Нет валидных сплитов "
            f"({len(xrj)} строк) — dir_jump пропущен"
        )

    print("  GATE (AUC, maximize)")
    gate_cands = (
        ALL_GATE_MODELS
        if MODELS_TO_TUNE_GATE is None
        else [m for m in ALL_GATE_MODELS if m in MODELS_TO_TUNE_GATE]
    )
    gate_cands = [m for m in gate_cands if m != "lgbm" or _HAS_LGBM]

    gate_out: dict[str, Any] = {}
    for mname in gate_cands:
        sn = f"{name}_gate_{mname}"
        res = tune_gate_one(
            mname, xg, yg, splits_gate, n_trials, timeout_sec, sn
        )
        am = res["all_metrics_at_best"]
        print(
            f"    {mname:8}: "
            f"AUC={am.get('auc', float('nan')):.4f} | "
            f"trials={res['n_trials']} | {res['elapsed_sec']:.0f}s"
        )
        gate_out[mname] = res

    print("  REG-JUMP (MAE, minimize, только скачки)")
    reg_cands = (
        ALL_REG_MODELS
        if MODELS_TO_TUNE_REG is None
        else [m for m in ALL_REG_MODELS if m in MODELS_TO_TUNE_REG]
    )
    reg_cands = [
        m for m in reg_cands if m != "lgbm_huber" or _HAS_LGBM
    ]

    dir_jump_out: dict[str, Any] = {}
    if splits_jump:
        for mname in reg_cands:
            sn = f"{name}_dir_jump_{mname}"
            res = tune_reg_one(
                mname, xrj, yrj, splits_jump, n_trials, timeout_sec, sn
            )
            am = res["all_metrics_at_best"]
            print(
                f"    {mname:12}: "
                f"MAE={am.get('mae', float('nan')):.5f} "
                f"RMSE={am.get('rmse', float('nan')):.5f} "
                f"signA={am.get('sign_acc', float('nan')):.3f} | "
                f"trials={res['n_trials']} | {res['elapsed_sec']:.0f}s"
            )
            dir_jump_out[mname] = res
    else:
        print("  warn REG-JUMP пропущен (недостаточно данных)")

    return {
        TRAIN_TARGET: {
            "gate": gate_out,
            "dir_jump": dir_jump_out,
        }
    }


def _merge_and_save(
    output_file: str,
    new_results: dict[str, Any],
    meta: dict[str, Any],
) -> None:
    """Сливает new_results в существующий файл, сохраняя чужие секции."""
    merged: dict[str, Any] = {}
    old_meta: dict[str, Any] = {}
    if os.path.exists(output_file):
        try:
            with open(output_file) as f:
                old = json.load(f)
            merged = old.get("results", {}) or {}
            old_meta = old.get("_meta", {}) or {}
            print(
                f"  слияние со старым {output_file} от "
                f"{str(old_meta.get('created', '?'))[:16]}"
            )
        except Exception as exc:
            print(f"  warn {output_file} нечитаем ({exc}) — перезапись")
            merged = {}
    for tf, by_t in new_results.items():
        merged.setdefault(tf, {})
        for tcol, by_s in by_t.items():
            merged[tf].setdefault(tcol, {})
            for stage, by_m in by_s.items():
                merged[tf][tcol][stage] = by_m
    meta = dict(meta)
    meta["previous_created"] = old_meta.get("created")
    payload = {"_meta": meta, "results": merged}
    with open(output_file, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"  сохранено: {output_file}")


def main() -> None:
    if not _HAS_LGBM:
        print("  warn lightgbm не установлен — lgbm / lgbm_huber пропущены")

    budget = BUDGET_CFG[BUDGET]
    n_studies_per_tf = len(ALL_GATE_MODELS) + len(ALL_REG_MODELS)
    total_studies = 2 * n_studies_per_tf

    print("ТЮНИНГ v9 — gate + reg_jump")
    print(
        f"  бюджет '{BUDGET}' | trials/study: {budget['n_trials']} | "
        f"timeout: {budget['timeout_min']} мин"
    )
    print(f"  gate-модели: {ALL_GATE_MODELS}")
    print(f"  reg-модели:  {ALL_REG_MODELS}")
    print(f"  studies всего: {total_studies} (2 ТФ x {n_studies_per_tf})")
    print(f"  старт: {datetime.now():%Y-%m-%d %H:%M:%S}")

    _resolve_path(DATA_1H, "1H-all")
    _resolve_path(DATA_6H, "6H-all")
    _resolve_path(DATA_1H_JUMP, "1H-jump")
    _resolve_path(DATA_6H_JUMP, "6H-jump")

    t_start = time.time()
    result: dict[str, Any] = {
        "1h": tune_timeframe(
            "1h",
            path_all=DATA_1H,
            path_jump=DATA_1H_JUMP,
            static_thresh=STATIC_THRESH_1H,
            embargo=EMBARGO_1H,
            budget=budget,
        ),
        "6h": tune_timeframe(
            "6h",
            path_all=DATA_6H,
            path_jump=DATA_6H_JUMP,
            static_thresh=STATIC_THRESH_6H,
            embargo=EMBARGO_6H,
            budget=budget,
        ),
    }
    total_min = (time.time() - t_start) / 60

    gate_results: dict[str, Any] = {}
    reg_results: dict[str, Any] = {}
    for tf, by_t in result.items():
        stages = by_t[TRAIN_TARGET]
        gate_results[tf] = {TRAIN_TARGET: {"gate": stages["gate"]}}
        reg_results[tf] = {
            TRAIN_TARGET: {"dir_jump": stages["dir_jump"]}
        }

    base_meta = {
        "created": datetime.now().isoformat(timespec="seconds"),
        "budget": BUDGET,
        "n_trials_per_combo": budget["n_trials"],
        "training_target": TRAIN_TARGET,
        "cv": {
            "type": "purged_walk_forward_embargo",
            "n_splits": N_WF_SPLITS,
            "embargo_1h": EMBARGO_1H,
            "embargo_6h": EMBARGO_6H,
        },
        "total_minutes": round(total_min, 1),
    }
    gate_meta = {
        **base_meta,
        "stage": "gate",
        "objective": GATE_OBJECTIVE,
        "models": MODELS_TO_TUNE_GATE or ALL_GATE_MODELS,
        "note": "v9: gate(AUC)",
    }
    reg_meta = {
        **base_meta,
        "stage": "dir_jump",
        "objective": DIR_OBJECTIVE,
        "models": MODELS_TO_TUNE_REG or ALL_REG_MODELS,
        "note": "v9: reg_jump(MAE, только скачки + price_ctx)",
    }

    print(f"  ИТОГ ТЮНИНГА — {total_min:.1f} мин")
    _merge_and_save(GATE_OUTPUT_FILE, gate_results, gate_meta)
    _merge_and_save(REG_OUTPUT_FILE, reg_results, reg_meta)

    print(
        f"  {'TF':<4}{'STAGE':<10}{'модель':<13}{'BEST':>10}{'trials':>8}"
    )
    for tf, by_t in result.items():
        for tcol, by_s in by_t.items():
            for stage in ["gate", "dir_jump"]:
                for mname, r in by_s.get(stage, {}).items():
                    am = r["all_metrics_at_best"]
                    if stage == "gate":
                        score_str = (
                            f"AUC={am.get('auc', float('nan')):.4f}"
                        )
                    else:
                        score_str = (
                            f"MAE={am.get('mae', float('nan')):.5f} "
                            f"signA="
                            f"{am.get('sign_acc', float('nan')):.3f}"
                        )
                    print(
                        f"  {tf.upper():<4}{stage:<10}{mname:<13}"
                        f"  {score_str:<28}{r['n_trials']:>8}"
                    )

    print(
        "  Структура JSON:\n"
        "    best_params.json:     "
        "results[tf][t_static]['gate'][model]\n"
        "    best_params_reg.json: "
        "results[tf][t_static]['dir_jump'][model]"
    )


def run_tuning(features_dir=None, models_dir=None) -> None:
    """Run Optuna tuning with configurable dataset paths."""
    global DATA_1H, DATA_6H, DATA_1H_JUMP, DATA_6H_JUMP
    global GATE_OUTPUT_FILE, REG_OUTPUT_FILE

    if features_dir is not None:
        features_dir = str(features_dir)
        DATA_1H = f"{features_dir}/final_dataset_1h.csv"
        DATA_6H = f"{features_dir}/final_dataset_6h.csv"
        DATA_1H_JUMP = f"{features_dir}/final_dataset_1h_jump.csv"
        DATA_6H_JUMP = f"{features_dir}/final_dataset_6h_jump.csv"
    if models_dir is not None:
        models_dir = str(models_dir)
        GATE_OUTPUT_FILE = f"{models_dir}/best_params.json"
        REG_OUTPUT_FILE = f"{models_dir}/best_params_reg.json"

    main()


if __name__ == "__main__":
    main()