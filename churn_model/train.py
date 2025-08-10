from .gbdt import train_gbdt, save_gbdt
from .lstm import NumpyLSTMClassifier, save_lstm
from .ensemble import EnsembleModel


def train(X_static, X_seq, y, gbdt_params=None, lstm_hidden=16, epochs=5, lr=0.01):
    """Train GBDT and LSTM models and return an ensemble."""
    gbdt_model = train_gbdt(X_static, y, gbdt_params)
    lstm_model = NumpyLSTMClassifier(X_seq.shape[2], lstm_hidden)
    lstm_model.fit(X_seq, y, epochs=epochs, lr=lr)
    return EnsembleModel(gbdt_model, lstm_model)


def save_model(model, base_path):
    save_gbdt(model.gbdt, base_path + '_gbdt.joblib')
    save_lstm(model.lstm, base_path + '_lstm.joblib')
