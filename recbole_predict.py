import argparse
import json
import warnings
import pandas as pd
from hyperopt import fmin, tpe, Trials, space_eval
from recbole.config import Config
from recbole.data import create_dataset, data_preparation
from recbole.utils import init_seed, get_model, get_trainer
import torch.distributed as dist
from hyperparams import ItemKNN, BPR, MultiVAE, SGL, NeuMF


def objectiveFunction(config):
    if config["library"] == "RecBole":
        return recboleObjFn(config)
    elif config["library"] == "LensKit":
        return lenskitObjFn(config)
    print("Unknown Library")
    return 0

def recboleObjFn(config_dict):
    config = Config(config_dict=config_dict, config_file_list=["recbole_settings.yaml"])
    init_seed(config["seed"], config["reproducibility"])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        dataset = create_dataset(config)
        train_data, _, test_data = data_preparation(config, dataset)
    model = get_model(config["model"])(config, train_data._dataset).to(config["device"])
    trainer = get_trainer(config["MODEL_TYPE"], config["model"])(config, model)
    print("Starting next run...")
    score, _ = trainer.fit(train_data, test_data, saved=False, show_progress=True)
    if not config["single_spec"]:
        print("tmp")
        dist.destroy_process_group()
    print(f"Config: {config_dict}")
    print(f"nDCG: {score}")
    print("-----")
    return -score

def lenskitObjFn(config):
    pass

def saveTrials(space, trials: Trials):
    filename = f"{space['library']}_{space['model']}_{space['dataset']}"
    trial_list = []
    for trial in trials.trials:
        params = {}
        for key, val in trial["misc"]["vals"].items():
            params[key] = val[0]
        params = space_eval(space, params)
        params.pop("dataset")
        params.pop("model")
        params.pop("library")
        trial_list.append({
            "parameter": params,
            "ndcg": -trial["result"]["loss"]
        })
    df = pd.DataFrame(trial_list)
    df.sort_values(by=["ndcg"], inplace=True)
    df.to_pickle(f"results/{filename}.pkl")

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Recbole Predict")
    parser.add_argument("--dataset", dest="dataset", type=str, required=True)
    parser.add_argument("--algorithm", dest="algorithm", type=str, required=True)
    parser.add_argument("--seed", dest="seed", type=int, required=True)
    args = parser.parse_args()
    algorithm = args.algorithm
    settings = json.load(open(f"./settings.json"))
    if algorithm == "ItemKNN":
        space = ItemKNN
    elif algorithm == "BPR":
        space = BPR
    elif algorithm == "MultiVAE":
        space = MultiVAE
    elif algorithm == "SGL":
        space = SGL
    elif algorithm == "NeuMF":
        space = NeuMF
    else:
        print("Algorithm not implemented.")
        space = {}

    space["dataset"] = args.dataset
    space["seed"] = args.seed
    space["seed"] = 42
    trials = Trials()
    best = fmin(objectiveFunction,
            space,
            algo=tpe.suggest,
            max_evals=settings["hyperopt_runs"],
            trials=trials)
    saveTrials(space, trials)
