import joblib
import autograd.numpy as np
from autograd import grad
from autograd.misc import flatten


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def tanh(x):
    return np.tanh(x)


class NumpyLSTMClassifier:
    """Minimal LSTM classifier implemented with autograd."""

    def __init__(self, input_dim, hidden_dim):
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.params = self._init_params()
        self.flat_params, self.unflatten = flatten(self.params)

    def _init_params(self):
        rng = np.random.default_rng(0)
        def rand(shape):
            return rng.normal(0, 0.1, size=shape)
        return {
            'Wf': rand((self.hidden_dim, self.input_dim)),
            'Uf': rand((self.hidden_dim, self.hidden_dim)),
            'bf': np.zeros((self.hidden_dim,)),
            'Wi': rand((self.hidden_dim, self.input_dim)),
            'Ui': rand((self.hidden_dim, self.hidden_dim)),
            'bi': np.zeros((self.hidden_dim,)),
            'Wo': rand((self.hidden_dim, self.input_dim)),
            'Uo': rand((self.hidden_dim, self.hidden_dim)),
            'bo': np.zeros((self.hidden_dim,)),
            'Wc': rand((self.hidden_dim, self.input_dim)),
            'Uc': rand((self.hidden_dim, self.hidden_dim)),
            'bc': np.zeros((self.hidden_dim,)),
            'Wy': rand((1, self.hidden_dim)),
            'by': np.zeros((1,)),
        }

    def _forward_single(self, x, params):
        h = np.zeros((self.hidden_dim,))
        c = np.zeros((self.hidden_dim,))
        for xt in x:
            f = sigmoid(params['Wf'] @ xt + params['Uf'] @ h + params['bf'])
            i = sigmoid(params['Wi'] @ xt + params['Ui'] @ h + params['bi'])
            o = sigmoid(params['Wo'] @ xt + params['Uo'] @ h + params['bo'])
            g = tanh(params['Wc'] @ xt + params['Uc'] @ h + params['bc'])
            c = f * c + i * g
            h = o * tanh(c)
        y_hat = sigmoid(params['Wy'] @ h + params['by'])
        return y_hat[0]

    def loss(self, flat_params, X, y):
        params = self.unflatten(flat_params)
        preds = [self._forward_single(x, params) for x in X]
        preds = np.clip(np.array(preds), 1e-7, 1 - 1e-7)
        return -np.mean(y * np.log(preds) + (1 - y) * np.log(1 - preds))

    def fit(self, X, y, epochs=5, lr=0.01):
        loss_grad = grad(self.loss)
        flat_params = self.flat_params
        for _ in range(epochs):
            grads = loss_grad(flat_params, X, y)
            flat_params = flat_params - lr * grads
        self.flat_params = flat_params
        self.params = self.unflatten(flat_params)

    def predict_proba(self, X):
        params = self.unflatten(self.flat_params)
        preds = [self._forward_single(x, params) for x in X]
        return np.array(preds)


def save_lstm(model, path):
    joblib.dump({'input_dim': model.input_dim,
                 'hidden_dim': model.hidden_dim,
                 'flat_params': model.flat_params}, path)


def load_lstm(path):
    data = joblib.load(path)
    model = NumpyLSTMClassifier(data['input_dim'], data['hidden_dim'])
    model.flat_params = data['flat_params']
    model.params = model.unflatten(model.flat_params)
    return model
