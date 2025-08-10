import numpy as np
from sklearn.metrics import roc_auc_score, average_precision_score


def precision_at_k(y_true, y_score, k):
    idx = np.argsort(y_score)[::-1][:k]
    return np.mean(y_true[idx])


def compute_metrics(y_true, y_score, k=10):
    return {
        'roc_auc': roc_auc_score(y_true, y_score),
        'pr_auc': average_precision_score(y_true, y_score),
        'precision_at_k': precision_at_k(y_true, y_score, k),
    }
