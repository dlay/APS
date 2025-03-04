from hyperopt import hp

ItemKNN = {
    "library": "RecBole",
    "model": "ItemKNN",
    "k": hp.choice("k", [10, 20, 50, 100, 200]),
    "shrink": hp.choice("shrink", [0.0, 0.1, 0.5, 1, 2])
}

BPR = {
    "library": "RecBole",
    "model": "BPR",
    "learning_rate": hp.choice("learning_rate", [5e-5, 1e-4, 5e-4, 7e-4, 1e-3, 5e-3, 7e-3]),
    "embedding_size": hp.choice("embedding_size", [32, 64, 128])
}

MultiVAE = {
    "library": "RecBole",
    "model": "MultiVAE",
    "learning_rate": hp.choice("learning_rate", [5e-5, 1e-4, 5e-4, 7e-4, 1e-3, 5e-3, 7e-3]),
    "drop_ratio": hp.choice("drop_ratio", [0.1, 0.2, 0.4, 0.5])
}

SGL = {
    "library": "RecBole",
    "model": "SGL",
    "ssl_tau": hp.choice("ssl_tau", [0.1, 0.2, 0.5]),
    "drop_ratio": hp.choice("drop_ratio", [0.1, 0.2, 0.4, 0.5]),
    "ssl_weight": hp.choice("ssl_weight", [0.05, 0.1, 0.5])
}

NeuMF = {
    "library": "RecBole",
    "model": "NeuMF",
    "learning_rate": hp.choice("learning_rate", [5e-7, 1e-6, 5e-6, 1e-5, 1e-4, 1e-3]),
    "mlp_hidden_size": hp.choice("mlp_hidden_size", ['[128, 64]', '[128, 64, 32]', '[64, 32, 16]']),
    "dropout_prob": hp.choice("dropout_prob", [0.0, 0.25, 0.5])
}
