import joblib
from sklearn.ensemble import HistGradientBoostingClassifier


def train_gbdt(X, y, params=None):
    """Train a GBDT model using scikit-learn's HistGradientBoostingClassifier.

    Args:
        X (array-like): Feature matrix for static features.
        y (array-like): Binary labels.
        params (dict, optional): Parameters for the classifier.

    Returns:
        HistGradientBoostingClassifier: Trained model.
    """
    params = params or {}
    model = HistGradientBoostingClassifier(**params)
    model.fit(X, y)
    return model


def save_gbdt(model, path):
    joblib.dump(model, path)


def load_gbdt(path):
    return joblib.load(path)
