
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
train_churn_with_report.py

Upgraded version of the training pipeline that:
- Splits data into Train/Validation/Test
- Tunes hyperparameters on Validation (Optuna)
- Trains final model
- Evaluates on Test: AUC, AP, Confusion Matrix at several thresholds
- Plots ROC and PR curves (matplotlib, no seaborn, one plot per figure)
- Saves artifacts:
    /artifacts/model.joblib
    /artifacts/metrics.json
    /artifacts/roc_curve.png
    /artifacts/pr_curve.png
    /artifacts/confusion_matrix_threshold_{t}.png
    /artifacts/report.md
"""
import os
import json
import numpy as np
import pandas as pd
import joblib
import optuna
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (
    roc_auc_score, average_precision_score, roc_curve, precision_recall_curve,
    confusion_matrix, precision_score, recall_score, f1_score
)
from sklearn.model_selection import train_test_split
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

FEATURES = [
    'fail_cnt_14d','succ_cnt_14d','coupon_amt_14d','refund_amt_14d','avg_amt_14d',
    'fail_cnt_30d','succ_cnt_30d','coupon_amt_30d','refund_amt_30d','avg_amt_30d',
    'switch_cnt_14d','switch_cnt_30d','downgraded_30d',
    'cancel_keyword_search_14d','faq_cancel_views_14d','cancel_page_visit_14d'
]

ARTIFACT_DIR = "/artifacts"
FEATURES_PATH = "/data/features/churn/dt=current.parquet"

def load_data():
    df = pd.read_parquet(FEATURES_PATH)
    if 'label' not in df.columns:
        raise RuntimeError("Expected 'label' column in features parquet.")
    X = df[FEATURES].fillna(0.0).astype(float).values
    y = df['label'].astype(int).values
    return X, y

def tune_hyperparams(X_tr, y_tr, X_va, y_va, n_trials=25):
    def objective(trial):
        clf = HistGradientBoostingClassifier(
            learning_rate=trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            max_iter=trial.suggest_int("max_iter", 200, 800),
            max_depth=trial.suggest_int("max_depth", 3, 20),
            l2_regularization=trial.suggest_float("l2_regularization", 0.0, 1.0),
            min_samples_leaf=trial.suggest_int("min_samples_leaf", 10, 100),
            class_weight="balanced"
        )
        clf.fit(X_tr, y_tr)
        prob = clf.predict_proba(X_va)[:, 1]
        ap = average_precision_score(y_va, prob) if len(np.unique(y_va)) > 1 else 0.0
        return ap
    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials)
    return study.best_params

def plot_roc(y_true, y_prob, path_png):
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    plt.figure()
    plt.plot(fpr, tpr, label="ROC")
    plt.plot([0,1], [0,1], linestyle="--")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path_png, dpi=150)
    plt.close()

def plot_pr(y_true, y_prob, path_png):
    precision, recall, _ = precision_recall_curve(y_true, y_prob)
    plt.figure()
    plt.plot(recall, precision, label="PR")
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title("Precision-Recall Curve")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path_png, dpi=150)
    plt.close()

def save_confusion_matrix_plot(y_true, y_prob, threshold, path_png):
    y_pred = (y_prob >= threshold).astype(int)
    cm = confusion_matrix(y_true, y_pred, labels=[0,1])
    tn, fp, fn, tp = cm.ravel()
    plt.figure()
    plt.imshow(cm, interpolation="nearest")
    plt.title(f"Confusion Matrix @ thr={threshold:.2f}")
    plt.xticks([0,1], ["Pred 0","Pred 1"])
    plt.yticks([0,1], ["True 0","True 1"])
    for (i, j), v in np.ndenumerate(cm):
        plt.text(j, i, str(v), ha="center", va="center")
    plt.tight_layout()
    plt.savefig(path_png, dpi=150)
    plt.close()
    prec = precision_score(y_true, y_pred, zero_division=0)
    rec = recall_score(y_true, y_pred, zero_division=0)
    f1  = f1_score(y_true, y_pred, zero_division=0)
    return {"tn":int(tn),"fp":int(fp),"fn":int(fn),"tp":int(tp),"precision":float(prec),"recall":float(rec),"f1":float(f1)}

def main():
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    X, y = load_data()

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, shuffle=False)
    X_tr, X_va, y_tr, y_va = train_test_split(X_tr, y_tr, test_size=0.2, shuffle=False)

    best = tune_hyperparams(X_tr, y_tr, X_va, y_va, n_trials=25)

    clf = HistGradientBoostingClassifier(**best, class_weight="balanced")
    clf.fit(X_tr, y_tr)

    y_prob = clf.predict_proba(X_te)[:, 1]
    auc = roc_auc_score(y_te, y_prob) if len(np.unique(y_te)) > 1 else float("nan")
    ap  = average_precision_score(y_te, y_prob) if len(np.unique(y_te)) > 1 else float("nan")

    roc_path = os.path.join(ARTIFACT_DIR, "roc_curve.png")
    pr_path  = os.path.join(ARTIFACT_DIR, "pr_curve.png")
    plot_roc(y_te, y_prob, roc_path)
    plot_pr(y_te, y_prob, pr_path)

    thresholds = [0.3, 0.5, 0.7, 0.8]
    cm_stats = {}
    for t in thresholds:
        p = os.path.join(ARTIFACT_DIR, f"confusion_matrix_threshold_{t:.2f}.png")
        cm_stats[f"{t:.2f}"] = save_confusion_matrix_plot(y_te, y_prob, t, p)

    joblib.dump({"model": clf, "features": FEATURES, "metrics": {"auc": float(auc), "ap": float(ap)}}, os.path.join(ARTIFACT_DIR, "model.joblib"))

    metrics = {
        "auc": float(auc),
        "ap": float(ap),
        "thresholds": cm_stats,
        "best_params": best
    }
    with open(os.path.join(ARTIFACT_DIR, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    report_md = f"""
# Churn Model Validation Report

## Summary
- **AUC**: {auc:.4f}
- **AP**: {ap:.4f}

## Curves
- ROC: `roc_curve.png`
- PR: `pr_curve.png`

## Confusion Matrices
"""
    for t, s in cm_stats.items():
        report_md += f"""
### Threshold = {t}
- Precision: {s['precision']:.4f}, Recall: {s['recall']:.4f}, F1: {s['f1']:.4f}
- TN: {s['tn']}, FP: {s['fp']}, FN: {s['fn']}, TP: {s['tp']}
- Image: `confusion_matrix_threshold_{t}.png`
"""
    with open(os.path.join(ARTIFACT_DIR, "report.md"), "w", encoding="utf-8") as f:
        f.write(report_md.strip())

    print("Artifacts saved to", ARTIFACT_DIR)

if __name__ == "__main__":
    main()
