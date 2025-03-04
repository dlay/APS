import os
import pandas as pd
import numpy as np

dir = "./omni-out/out"
missing = {"alg": [], "dataset": []}
data = {"alg": [], "dataset": [], "ndcg": [], "config": []}
finished = {"alg": [], "dataset": []}

def mean_absolute_distance(arr):
    arr = arr[~np.isnan(arr)]
    n = len(arr)
    arr = arr.reset_index(drop=True)
    
    if n <= 1:
        return 0
    
    total = 0
    for i in range(n):
        for j in range(i + 1, n):
            total += abs(arr[i] - arr[j])
    return total / (n * (n - 1) / 2)

if __name__ == "__main__":
    for file in os.listdir(os.fsencode(dir)):
        filename = os.fsdecode(file)
        configs = []
        ndcgs = []
        with open(f"{dir}/{filename}") as f:
            for line in f:
                if line.startswith("Config:"):
                    configs.append(line[8:-1])
                if line.startswith("nDCG:"):
                    ndcgs.append(line[6:-1])
        alg = filename.split(".")[0].split("_")[1]
        ds = filename.split(".")[0].split("_")
        ds.pop(0)
        ds.pop(0)
        if len(ds) == 1:
            dataset = ds[0]
        else:
            dataset = "_".join(ds)
        if len(ndcgs) == 0:
            missing["alg"].append(alg)
            missing["dataset"].append(dataset)
            continue
        if len(ndcgs) == 20:
            finished["alg"].append(alg)
            finished["dataset"].append(dataset)
        imax = max(range(len(ndcgs)), key=ndcgs.__getitem__)
        data["alg"].append(alg)
        data["dataset"].append(dataset)
        data["ndcg"].append(ndcgs[imax])
        data["config"].append(configs[imax])

    df = pd.DataFrame(data)
    df["ndcg"] = pd.to_numeric(df["ndcg"])
    df.to_csv("./results-preformat.csv")
    df = pd.pivot_table(df, index="dataset", columns="alg", values="ndcg", aggfunc="mean")
    df["Difficulty"] = 1 - df.mean(axis=1)
    df["Variance"] = df[["BPR", "ItemKNN", "MultiVAE", "SGL", "NeuMF"]].apply(mean_absolute_distance, axis=1)
    df = df.sort_values(by="Difficulty").round(4)
    df.to_csv("./results.csv")
    df2 = pd.DataFrame(missing)
    df2.to_csv("./missing.csv")
    df3 = pd.DataFrame(finished)
    df3.to_csv("./finished.csv")
