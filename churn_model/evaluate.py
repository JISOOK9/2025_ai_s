from .metrics import compute_metrics


def evaluate(model, X_static, X_seq, y, k=10):
    """Evaluate ensemble model and return metrics."""
    scores = model.predict_proba(X_static, X_seq)
    return compute_metrics(y, scores, k)
