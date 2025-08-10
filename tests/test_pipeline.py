import numpy as np
from churn_model import train, evaluate, finetune


def generate_data(n=50, seq_len=5, static_dim=3, seq_dim=2):
    rng = np.random.default_rng(0)
    X_static = rng.normal(size=(n, static_dim))
    X_seq = rng.normal(size=(n, seq_len, seq_dim))
    y = (rng.random(n) > 0.5).astype(int)
    return X_static, X_seq, y


def test_train_evaluate():
    Xs, Xseq, y = generate_data()
    model = train(Xs, Xseq, y, gbdt_params={'max_depth': 2}, lstm_hidden=4, epochs=1, lr=0.1)
    metrics = evaluate(model, Xs, Xseq, y, k=5)
    assert set(['roc_auc', 'pr_auc', 'precision_at_k']).issubset(metrics.keys())


def test_finetune():
    Xs, Xseq, y = generate_data()
    study = finetune(Xs, Xseq, y, n_trials=1)
    assert len(study.trials) == 1
