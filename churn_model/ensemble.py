import numpy as np


class EnsembleModel:
    """Simple averaging ensemble for GBDT and LSTM models."""

    def __init__(self, gbdt_model, lstm_model):
        self.gbdt = gbdt_model
        self.lstm = lstm_model

    def predict_proba(self, X_static, X_seq):
        gbdt_pred = self.gbdt.predict_proba(X_static)[:, 1]
        lstm_pred = self.lstm.predict_proba(X_seq)
        return (gbdt_pred + lstm_pred) / 2
