import optuna
from .train import train
from .evaluate import evaluate


def finetune(X_static, X_seq, y, n_trials=5):
    """Hyperparameter tuning using Optuna."""
    def objective(trial):
        gbdt_params = {
            'max_depth': trial.suggest_int('max_depth', 2, 6),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
        }
        lstm_hidden = trial.suggest_int('lstm_hidden', 4, 32)
        model = train(X_static, X_seq, y, gbdt_params, lstm_hidden, epochs=3, lr=0.05)
        metrics = evaluate(model, X_static, X_seq, y, k=10)
        return metrics['roc_auc']

    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials)
    return study
