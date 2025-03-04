import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist

def normalized_pairwise_distance_variance(points):
    points_count, dim = points.shape

    if points_count <= 1:
        return 0.0

    # Compute pairwise distances
    pairwise_distances = pdist(points, metric='euclidean')

    # Compute variance of the pairwise distances
    variance = np.var(pairwise_distances)
    
    # Theoretical maximum variance approximation
    max_distance = np.sqrt(dim)  # Distance between two opposite corners in [0, 1]^d
    min_distance = 0           # Distance for perfectly clustered points
    max_possible_variance = (max_distance - min_distance)**2 / 4  # Worst-case spread

    # Normalize the variance to [0, 1]
    normalized_variance = 1 - (variance / max_possible_variance)

    # Compute the coverage factor
    ranges = np.ptp(points, axis=0)
    coverage = np.prod(ranges + 1e-12) ** (1/dim)

    # diversity metric combines normalized variance and coverage factor
    metric = normalized_variance * coverage

    return np.array([normalized_variance, coverage, metric])

if __name__ == "__main__":
    df = pd.read_csv("results.csv", index_col=0, usecols=["dataset", "BPR", "ItemKNN", "MultiVAE", "SGL", "NeuMF"])
    df = df.dropna().sort_index()
    datasets = df.index.to_list()

    sets = []
    data = {"datasets": [], "diversity": []}

    b2p = df[df.index.isin(["Jester", "Food"])].to_numpy() # best 2p set
    sets.append([["Jester", "Food"], b2p])
    b3p = df[df.index.isin(["Jester", "Food", "MovieLensLatestSmall"])].to_numpy() # best 3p set
    sets.append([["Jester", "Food", "MovieLensLatestSmall"], b3p])
    b4p = df[df.index.isin(["Jester", "Food", "Amazon_Magazine_Subscriptions", "FilmTrust"])].to_numpy() # best 4p set
    sets.append([["Jester", "Food", "Amazon_Magazine_Subscriptions", "FilmTrust"], b4p])
    w2p = df[df.index.isin(["FourSquareNYC", "MarketBiasModcloth"])].to_numpy() # worst 2p set
    sets.append([["FourSquareNYC", "MarketBiasModcloth"], w2p])
    w3p = df[df.index.isin(["Amazon_Musical_Instruments", "Amazon_Prime_Pantry", "RentTheRunway"])].to_numpy() # worst 3p set
    sets.append([["Amazon_Musical_Instruments", "Amazon_Prime_Pantry", "RentTheRunway"], w3p])
    w4p = df[df.index.isin(["Amazon_Arts_Crafts_and_Sewing", "Amazon_Digital_Music", "Food", "RentTheRunway"])].to_numpy() # worst 4p set
    sets.append([["Amazon_Arts_Crafts_and_Sewing", "Amazon_Digital_Music", "Food", "RentTheRunway"], w4p])
    ml = df[df.index.isin(["MovieLens1m", "MovieLens100k", "MovieLensLatestSmall"])].to_numpy() # movielens 3p set
    sets.append([["MovieLens1m", "MovieLens100k", "MovieLensLatestSmall"], ml]) 
    amzp = df[df.index.isin(['Amazon_Arts_Crafts_and_Sewing', 'Amazon_Digital_Music', 'Amazon_Gift_Cards'])].to_numpy() # amazon 3p set
    sets.append([['Amazon_Arts_Crafts_and_Sewing', 'Amazon_Digital_Music', 'Amazon_Gift_Cards'], amzp])
    outl = df[df.index.isin(['Jester', 'Amazon_Arts_Crafts_and_Sewing', 'Amazon_Digital_Music', 'Amazon_Gift_Cards'])].to_numpy() # amazon + outlier 4p set
    sets.append([['Jester', 'Amazon_Arts_Crafts_and_Sewing', 'Amazon_Digital_Music', 'Amazon_Gift_Cards'], outl])

    for set in sets:
        data["datasets"].append(set[0])
        result = normalized_pairwise_distance_variance(set[1])
        data["diversity"].append(result[2])
    
    rdf = pd.DataFrame(data)
    print(rdf.head(10))
    rdf.to_csv("diversity.csv")
