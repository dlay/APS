import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import math

if __name__ == "__main__":
    data = pd.read_csv("results-preformat.csv", usecols=["alg", "dataset", "ndcg"])
    alg_list = data["alg"].unique().tolist()
    data = data.groupby(["dataset", "alg"])["ndcg"].mean().to_frame().reset_index()

    with PdfPages("APS.pdf") as pdf:
        for alg1 in alg_list:
            h = 0
            w = 0
            dfx = data[data["alg"] == alg1]
            height = 2
            width = 2
            fig, axs = plt.subplots(height, width, layout="constrained", figsize=(6, 6))
            for alg2 in alg_list:
                if alg1 == alg2:
                    continue
                dfy = data[data["alg"] == alg2]
                merged = pd.merge(dfx, dfy, on="dataset")
                x = merged["ndcg_x"]
                y = merged["ndcg_y"]
                max_total = max(x.max(), y.max())

                # seperate movielens and amazon data for highlighting
                movielens = merged.query("dataset.str.contains('movielens', case=False)")
                amazon = merged.query("dataset.str.contains('amazon', case=False)")
                merged = pd.merge(merged, amazon, indicator=True, how="outer").query("_merge=='left_only'").drop("_merge", axis=1)
                merged = pd.merge(merged, movielens, indicator=True, how="outer").query("_merge=='left_only'").drop("_merge", axis=1)

                # normalize
                xm = movielens["ndcg_x"] / max_total
                ym = movielens["ndcg_y"] / max_total
                xa = amazon["ndcg_x"] / max_total
                ya = amazon["ndcg_y"] / max_total
                x = merged["ndcg_x"] / max_total
                y = merged["ndcg_y"] / max_total
                axs[h, w].set(xlabel=alg1, ylabel=alg2)
                axs[h, w].set_xlim(0, 1)
                axs[h, w].set_ylim(0, 1)
                axs[h, w].set_aspect("equal", "box")
                axs[h, w].plot([0, 1], [0, 1])
                axs[h, w].plot(x, y, "o", markersize=7)
                axs[h, w].plot(xm, ym, "mx", markersize=7)
                axs[h, w].plot(xa, ya, "k2", markersize=7)
                w = (w + 1) % 2
                if w == 0:
                    h += 1
                print(f"{alg1}:{alg2}={max_total:.2f}")

            plt.savefig(f"{alg1}")
            plt.title(alg1)
            pdf.savefig(fig)
            plt.close()
            print(f"Page done for {alg1}.")
        d = pdf.infodict()
        d['Title'] = 'Algorithm Performance Spaces'