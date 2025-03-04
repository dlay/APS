import pandas as pd
import numpy as np
import numpy.ma as ma
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import normalize, MinMaxScaler

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
    metric = "ndcg"
    label = False
    label_cutoff_x = 0.0
    save_fig = True
    highlight = False
    norm = False

    data = pd.read_csv("results-preformat.csv", usecols=["dataset", "alg", metric])
    data = data.groupby(["dataset", "alg"])[metric].mean().to_frame().reset_index()
        
    algorithms = data["alg"].unique().tolist()
    datasets = data["dataset"].unique().tolist()
    X = []
    for ds in datasets:
        row = []
        for alg in algorithms:
            result = data.loc[(data["alg"] == alg) & (data["dataset"] == ds), metric]
            if len(result) > 0:
                result = result.iat[0]
            else:
                result = np.nan
            row.append(result)
        X.append(row)
    df2 = pd.DataFrame(X)
    diff = 1 - df2.mean(axis=1)
    var = df2.apply(mean_absolute_distance, axis=1)

    imp = SimpleImputer(missing_values=np.nan, strategy="mean")
    imp.fit(X)
    X = imp.transform(X)
    pca = PCA(n_components=2)
    pca.fit(X)
    X_pca = pca.transform(X)
    dfvar = pd.DataFrame(diff)
    dfvar[1] = X_pca[:,1]
    variance = pca.explained_variance_ratio_
    print(f"Correlation Coefficient of Component 1 - Difficulty: {np.corrcoef(X_pca[:,0], diff)[0,1]}")
    print(f"Correlation Coefficient of Component 2 - Variance: {dfvar.corr()[0][1]}")
    print(f"Variance of PCA Components: {variance}")

    if norm:
        scaler = MinMaxScaler()
        scaler.fit(X_pca)
        X_pca = scaler.transform(X_pca)

    df = pd.DataFrame(X_pca, columns=["component_1", "component_2"], index=datasets)
    df.to_csv("pca_norm.csv")

    plt.gca().set_aspect("auto", "box")
    if highlight:
        movielens = df[df.index.str.contains("movielens", case=False)]
        amazon = df[df.index.str.contains("amazon", case=False)]
        merged = pd.merge(df, amazon, indicator=True, how="outer").query("_merge=='left_only'").drop("_merge", axis=1)
        merged = pd.merge(merged, movielens, indicator=True, how="outer").query("_merge=='left_only'").drop("_merge", axis=1)
        plt.plot("component_1", "component_2", "o", data=merged, markersize=4)
        plt.plot("component_1", "component_2", "mx", data=movielens, markersize=7)
        plt.plot("component_1", "component_2", "k2", data=amazon, markersize=7)
    else:
        cmap = plt.get_cmap('copper')
        plt.scatter(df["component_1"], df["component_2"], c=var, cmap=cmap)
        plt.colorbar(label="VarianceAPS")
    if label:
        for xy, ds in zip(X_pca, datasets):
            if xy[0] > label_cutoff_x:
                plt.text(xy[0], xy[1], ds, fontsize=6)
    plt.xlabel(f"Component 1 - {variance[0]:.2%}")
    plt.ylabel(f"Component 2 - {variance[1]:.2%}")
    if save_fig:
        plt.savefig("pca_raw_labeled_norand4")
    print(f"Datasets: {len(datasets)}, Algorithms: {len(algorithms)}, Variance C1: {variance[0]:.2%}, Variance C2: {variance[1]:.2%}")
    plt.show()
