import argparse
import json
import ast
import os
import re
import zipfile
import tarfile
import gzip
from collections import Counter
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

def readData(name: str):
    if name == "AliEC":
        return pd.read_csv(f"data/{name}/raw_sample.csv.tar.gz", compression="gzip", names=["user", "item", "rating"], header=0, dtype="Int64", usecols=[0, 2, 5], engine="c")
    if name == "Amazon_All_Beauty":
        return pd.read_csv(f"data/{name}/All_Beauty.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Appliances":
        return pd.read_csv(f"data/{name}/Appliances.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Arts_Crafts_and_Sewing":
        return pd.read_csv(f"data/{name}/Arts_Crafts_and_Sewing.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Automotive":
        return pd.read_csv(f"data/{name}/Automotive.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Books":
        return pd.read_csv(f"data/{name}/Books.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_CDs_and_Vinyl":
        return pd.read_csv(f"data/{name}/CDs_and_Vinyl.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Cell_Phones_and_Accessories":
        return pd.read_csv(f"data/{name}/Cell_Phones_and_Accessories.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Clothing_Shoes_and_Jewelry":
        return pd.read_csv(f"data/{name}/Clothing_Shoes_and_Jewelry.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Digital_Music":
        return pd.read_csv(f"data/{name}/Digital_Music.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Electronics":
        return pd.read_csv(f"data/{name}/Electronics.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Fashion":
        return pd.read_csv(f"data/{name}/AMAZON_FASHION.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Gift_Cards":
        return pd.read_csv(f"data/{name}/Gift_Cards.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Grocery_and_Gourmet_Food":
        return pd.read_csv(f"data/{name}/Grocery_and_Gourmet_Food.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Home_and_Kitchen":
        return pd.read_csv(f"data/{name}/Home_and_Kitchen.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Industrial_and_Scientific":
        return pd.read_csv(f"data/{name}/Industrial_and_Scientific.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Kindle_Store":
        return pd.read_csv(f"data/{name}/Kindle_Store.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Luxury_Beauty":
        return pd.read_csv(f"data/{name}/Luxury_Beauty.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Magazine_Subscriptions":
        return pd.read_csv(f"data/{name}/Magazine_Subscriptions.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Movies_and_TV":
        return pd.read_csv(f"data/{name}/Movies_and_TV.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Musical_Instruments":
        return pd.read_csv(f"data/{name}/Musical_Instruments.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Office_Products":
        return pd.read_csv(f"data/{name}/Office_Products.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Patio_Lawn_and_Garden":
        return pd.read_csv(f"data/{name}/Patio_Lawn_and_Garden.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Pet_Supplies":
        return pd.read_csv(f"data/{name}/Pet_Supplies.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Prime_Pantry":
        return pd.read_csv(f"data/{name}/Prime_Pantry.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Software":
        return pd.read_csv(f"data/{name}/Software.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Sports_and_Outdoors":
        return pd.read_csv(f"data/{name}/Sports_and_Outdoors.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Tools_and_Home_Improvement":
        return pd.read_csv(f"data/{name}/Tools_and_Home_Improvement.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Toys_and_Games":
        return pd.read_csv(f"data/{name}/Toys_and_Games.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Amazon_Video_Games":
        return pd.read_csv(f"data/{name}/Video_Games.csv", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "Anime":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("rating.csv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], header=0, usecols=[0, 1, 2], engine="python")
    if name == "BeerAdvocate":
        with gzip.open(f"data/{name}/beeradvocate.json.gz") as zfile:
            data = {"user": [], "item": [], "rating": []}
            for line in zfile:
                line = ast.literal_eval(json.dumps(line.decode("utf-8").replace("\'", "\"")))
                line = line[:line.find("review/text")-3] + "}"
                line = "{" + line[line.find("beer/beerId")-1:]
                line = json.loads(line)
                try:
                    user = line["review/profileName"]
                    item = line["beer/beerId"]
                    rating = line["review/overall"]
                except KeyError:
                    continue
                data["user"].append(user)
                data["item"].append(item)
                data["rating"].append(rating)

        df = pd.DataFrame(data)
        unique_ids = {key: value for value, key in enumerate(df["user"].unique())}
        df["user"].update(df["user"].map(unique_ids))
        df["user"] = pd.to_numeric(df["user"])
        df["item"] = pd.to_numeric(df["item"])
        df["rating"] = pd.to_numeric(df["rating"])
        return df
    if name == "Behance":
        return pd.read_csv(f"data/{name}/Behance_appreciate_1M.gz", compression="gzip", sep=" ", names=["user", "item"], usecols=[0, 1], engine="python")
    if name == "BookCrossing":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("Ratings.csv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], sep=";", header=0, usecols=[0, 1, 2], engine="python")
    if name == "CiaoDVD":
        with zipfile.ZipFile(f"data/{name}/CiaoDVD.zip") as zfile:
            with zfile.open("movie-ratings.txt") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], usecols=[0, 1, 4], engine="python")
    if name == "CiteULike-a":
        with zipfile.ZipFile(f"data/{name}/citeulike-a-master.zip") as zfile:
            with zfile.open("citeulike-a-master/users.dat") as file:
                data = []
                for user, line in enumerate(file.readlines()):
                    items = line.decode("utf-8").strip("\n").split(" ")[1:]
                    for item in items:
                        data.append((user, item))
                df = pd.DataFrame(data, columns=["user", "item"])
                df["item"] = pd.to_numeric(df["item"])
                return df
    if name == "CosmeticsShop":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            data = []
            with zfile.open("2019-Dec.csv") as file:
                next(file)
                for line in file:
                    elements = line.decode("utf-8").strip("\n").split(",")
                    if (elements[1] == "purchase"):
                        data.append((elements[7], elements[2]))
            with zfile.open("2019-Nov.csv") as file:
                next(file)
                for line in file:
                    elements = line.decode("utf-8").strip("\n").split(",")
                    if (elements[1] == "purchase"):
                        data.append((elements[7], elements[2]))
            with zfile.open("2019-Oct.csv") as file:
                next(file)
                for line in file:
                    elements = line.decode("utf-8").strip("\n").split(",")
                    if (elements[1] == "purchase"):
                        data.append((elements[7], elements[2]))
            with zfile.open("2020-Feb.csv") as file:
                next(file)
                for line in file:
                    elements = line.decode("utf-8").strip("\n").split(",")
                    if (elements[1] == "purchase"):
                        data.append((elements[7], elements[2]))
            with zfile.open("2020-Jan.csv") as file:
                next(file)
                for line in file:
                    elements = line.decode("utf-8").strip("\n").split(",")
                    if (elements[1] == "purchase"):
                        data.append((elements[7], elements[2]))
            df = pd.DataFrame(data, columns=["user", "item"])
            df["user"] = pd.to_numeric(df["user"])
            df["item"] = pd.to_numeric(df["item"])
            return df
    if name == "DeliveryHeroSE":
        with zipfile.ZipFile(f"data/{name}/data_se.zip") as zfile:
            with zfile.open("data_se/orders_se.txt") as file:
                return pd.read_csv(file, names=["user", "item"], usecols=[1, 5], header=0, engine="python")
    if name == "DeliveryHeroSG":
        with zipfile.ZipFile(f"data/{name}/data_sg.zip") as zfile:
            with zfile.open("data_sg/orders_sg.txt") as file:
                return pd.read_csv(file, names=["user", "item"], usecols=[1, 5], header=0, engine="python")
    if name == "DeliveryHeroTW":
        with zipfile.ZipFile(f"data/{name}/data_tw.zip") as zfile:
            with zfile.open("data_tw/orders_tw.txt") as file:
                return pd.read_csv(file, names=["user", "item"], usecols=[1, 5], header=0, engine="python")
    if name == "DoubanBook":
        with tarfile.open(f"data/{name}/Douban.tar.gz") as zfile:
            with zfile.extractfile("Douban/book/douban_book.tsv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], sep="\t", header=0, usecols=[0, 1, 2], engine="python")
    if name == "DoubanMovie":
        with tarfile.open(f"data/{name}/Douban.tar.gz") as zfile:
            with zfile.extractfile("Douban/movie/douban_movie.tsv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], sep="\t", header=0, usecols=[0, 1, 2], engine="python")
    if name == "DoubanMusic":
        with tarfile.open(f"data/{name}/Douban.tar.gz") as zfile:
            with zfile.extractfile("Douban/music/douban_music.tsv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], sep="\t", header=0, usecols=[0, 1, 2], engine="python")
    if name == "DoubanShort":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("DMSC.csv") as file:
                return pd.read_csv(file, names=["item", "user", "rating"], header=0, usecols=[5, 1, 7], engine="python").reindex(columns=["user", "item", "rating"])
    if name == "Epinions":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("epinions/rating.txt") as file:
                return pd.read_csv(file, names=["item", "user", "rating"], sep="\t", usecols=[0, 1, 2], engine="python").reindex(columns=["user", "item", "rating"])
    if name == "FilmTrust":
        with zipfile.ZipFile(f"data/{name}/filmtrust.zip") as zfile:
            with zfile.open("ratings.txt") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], sep=" ", usecols=[0, 1, 2], engine="python")
    if name == "Food":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("RAW_interactions.csv") as file:
                data = []
                next(file)
                for line in file:
                    line = re.search(r'^\d+,\d+,\d{4}-\d{2}-\d{2},\d', line.decode("utf-8"))
                    if not line:
                        continue
                    line = line.group().split(",")
                    if len(line) > 0:
                        data.append((line[0], line[1], line[3]))
                df = pd.DataFrame(data, columns=["user", "item", "rating"])
                df = df.apply(pd.to_numeric)
                return df
    if name == "FourSquareNYC":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("dataset_TSMC2014_NYC.csv") as file:
                return pd.read_csv(file, names=["user", "item"], header=0, usecols=[0, 1], engine="python")
    if name == "FourSquareTokyo":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("dataset_TSMC2014_TKY.csv") as file:
                return pd.read_csv(file, names=["user", "item"], header=0, usecols=[0, 1], engine="python")
    if name == "Globo":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            data = []
            for file in zfile.filelist:
                if file.filename.startswith("clicks/clicks/"):
                    with zfile.open(file.filename) as file:
                        next(file)
                        for line in file:
                            line = line.decode("utf-8").strip("\n").split(",")
                            data.append((line[0], line[4]))
            return pd.DataFrame(data, columns=["user", "item"])
    if name == "GoodReadsComics":
        with gzip.open(f"data/{name}/goodreads_interactions_comics_graphic.json.gz") as zfile:
            data = []
            for line in zfile.readlines():
                line = json.loads(line.decode("utf-8"))
                data.append((line["user_id"], line["book_id"], line["rating"]))
            return pd.DataFrame(data, columns=["user", "item", "rating"])
    if name == "GoogleLocalAlaska":
        return pd.read_csv(f"data/{name}/rating-Alaska.csv.gz", compression="gzip", names=["item", "user", "rating"], header=0, usecols=[0, 1, 2], engine="python").reindex(columns=["user", "item", "rating"])
    if name == "GoogleLocalDelaware":
        return pd.read_csv(f"data/{name}/rating-Delaware.csv.gz", compression="gzip", names=["item", "user", "rating"], header=0, usecols=[0, 1, 2], engine="python").reindex(columns=["user", "item", "rating"])
    if name == "GoogleLocalDistrictOfColumbia":
        return pd.read_csv(f"data/{name}/rating-District_of_Columbia.csv.gz", compression="gzip", names=["item", "user", "rating"], header=0, usecols=[0, 1, 2], engine="python").reindex(columns=["user", "item", "rating"])
    if name == "GoogleLocalMontana":
        return pd.read_csv(f"data/{name}/rating-Montana.csv.gz", compression="gzip", names=["item", "user", "rating"], header=0, usecols=[0, 1, 2], engine="python").reindex(columns=["user", "item", "rating"])
    if name == "GoogleLocalVermont":
        return pd.read_csv(f"data/{name}/rating-Vermont.csv.gz", compression="gzip", names=["item", "user", "rating"], header=0, usecols=[0, 1, 2], engine="python").reindex(columns=["user", "item", "rating"])
    if name == "Gowalla":
        return pd.read_csv(f"data/{name}/loc-gowalla_totalCheckins.txt.gz", compression="gzip", names=["user", "item"], sep="\t", usecols=[0, 4], engine="python")
    if name == "Jester":
        with zipfile.ZipFile(f"data/{name}/JesterDataset4.zip") as zfile:
            with zfile.open("[final] April 2015 to Nov 30 2019 - Transformed Jester Data - .xlsx") as file:
                df = pd.read_excel(file)
                df = df.iloc[:, 1:]
                df["user"] = [i for i in range(len(df))]
                df = df.melt(id_vars="user", var_name="item", value_name="rating")
                return df
    if name == "LastFM":
        with zipfile.ZipFile(f"data/{name}/hetrec2011-lastfm-2k.zip") as zfile:
            with zfile.open("user_artists.dat") as file:
                return pd.read_csv(file, names=["user", "item"], sep="\t", usecols=[0, 1], header=0, engine="python")
    if name == "LearningFromSets":
        with zipfile.ZipFile(f"data/{name}/learning-from-sets-2019.zip") as zfile:
            with zfile.open("learning-from-sets-2019/item_ratings.csv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], usecols=[0, 1, 2], header=0, engine="python")
    if name == "LibraryThing":
        with tarfile.open(f"data/{name}/lthing_data.tar.gz") as zfile:
            with zfile.extractfile("lthing_data/reviews.txt") as file:
                data = {"user": [], "item": [], "rating": []}
                next(file)
                for line in file:
                    line = eval(line.decode("utf-8").split("')] = ")[1])
                    try:
                        user = line["user"]
                        item = line["work"]
                        rating = line["stars"]
                    except KeyError:
                        continue
                    data["user"].append(user)
                    data["item"].append(item)
                    data["rating"].append(rating)
                return pd.DataFrame(data)
    if name == "MarketBiasModcloth":
        return pd.read_csv(f"data/{name}/df_modcloth.csv", names=["item", "user", "rating"], usecols=[0, 1, 2], header=0, engine="python").reindex(columns=["user", "item", "rating"])
    if name == "MillionSong":
        with zipfile.ZipFile(f"data/{name}/train_triplets.txt.zip") as zfile:
            with zfile.open("train_triplets.txt") as file:
                return pd.read_csv(file, names=["user", "item"], usecols=[0, 1], sep="\t", engine="python")
    if name == "MIND-Small":
        data = {"user": [], "item": []}
        with zipfile.ZipFile(f"data/{name}/MINDsmall_train.zip") as zfile:
            with zfile.open("behaviors.tsv") as file:
                for line in file:
                    line = line.decode("utf-8").split("\t")
                    user = line[1]
                    items = line[3].split(" ")
                    extra = [s for s in line[4].split(" ") if "-1" in s]
                    if len(extra) > 0:
                        items.append(extra[0].split("-")[0])
                    for item in items:
                        data["user"].append(user)
                        data["item"].append(item)
        with zipfile.ZipFile(f"data/{name}/MINDsmall_dev.zip") as zfile:
            with zfile.open("behaviors.tsv") as file:
                for line in file:
                    line = line.decode("utf-8").split("\t")
                    user = line[1]
                    items = line[3].split(" ")
                    extra = [s for s in line[4].split(" ") if "-1" in s]
                    if len(extra) > 0:
                        items.append(extra[0].split("-")[0])
                    for item in items:
                        data["user"].append(user)
                        data["item"].append(item)
        return pd.DataFrame(data)
    if name == "ModCloth":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("modcloth_final_data.json") as file:
                df = pd.read_json(file, lines=True)[["user_id", "item_id", "quality"]]
                df.rename(columns={"user_id": "user", "item_id": "item", "quality": "rating"}, inplace=True)
                return df
    if name == "MovieLens1m":
        with zipfile.ZipFile(f"data/{name}/ml-1m.zip") as zfile:
            with zfile.open("ml-1m/ratings.dat") as file:
                return pd.read_csv(file, sep="::", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "MovieLens100k":
        with zipfile.ZipFile(f"data/{name}/ml-100k.zip") as zfile:
            with zfile.open("ml-100k/u.data") as file:
                return pd.read_csv(file, sep="\t", names=["user", "item", "rating"], usecols=[0, 1, 2], engine="python")
    if name == "MovieLensLatestSmall":
        with zipfile.ZipFile(f"data/{name}/ml-latest-small.zip") as zfile:
            with zfile.open("ml-latest-small/ratings.csv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], usecols=[0, 1, 2], header=0, engine="python")
    if name == "MovieTweetings":
        return pd.read_csv(f"data/{name}/ratings.dat", names=["user", "item", "rating"], sep="::", usecols=[0, 1, 2], engine="python")
    if name == "Netflix":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            data = {"user": [], "item": [], "rating": []}
            for i in range(4):
                with zfile.open(f"combined_data_{i+1}.txt") as file:
                    item = 0
                    for line in file:
                        line = line.decode("utf-8").split(",")
                        if (len(line) == 1):
                            item = line[0].split(":")[0]
                            continue
                        data["user"].append(line[0])
                        data["item"].append(item)
                        data["rating"].append(line[1])
            df = pd.DataFrame(data)
            df["user"] = pd.to_numeric(df["user"])
            df["item"] = pd.to_numeric(df["item"])
            df["rating"] = pd.to_numeric(df["rating"])
            return df
    if name == "RateBeer":
        with gzip.open(f"data/{name}/ratebeer.json.gz") as zfile:
            data = {"user": [], "item": [], "rating": []}
            for line in zfile:
                line = ast.literal_eval(json.dumps(line.decode("utf-8").replace("\'", "\"")))
                line = line[:line.find("review/text")-3] + "}"
                line = "{" + line[line.find("beer/beerId")-1:]
                line = json.loads(line)
                try:
                    user = line["review/profileName"]
                    item = line["beer/beerId"]
                    rating = line["review/overall"].split("/")[0]
                except KeyError:
                    continue
                data["user"].append(user)
                data["item"].append(item)
                data["rating"].append(rating)

        df = pd.DataFrame(data)
        unique_ids = {key: value for value, key in enumerate(df["user"].unique())}
        df["user"].update(df["user"].map(unique_ids))
        df["user"] = pd.to_numeric(df["user"])
        unique_ids = {key: value for value, key in enumerate(df["item"].unique())}
        df["item"].update(df["item"].map(unique_ids))
        df["item"] = pd.to_numeric(df["item"])
        df["rating"] = pd.to_numeric(df["rating"])
        return df
    if name == "Rekko":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("ratings.csv") as file:
                return pd.read_csv(file, names=["user", "item", "rating"], usecols=[0, 1, 2], header=0, engine="python")
    if name == "RentTheRunway":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("renttherunway_final_data.json") as file:
                df = pd.read_json(file, lines=True)[["user_id", "item_id", "rating"]]
                df.rename(columns={"user_id": "user", "item_id": "item"}, inplace=True)
                return df
    if name == "Retailrocket":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("events.csv") as file:
                return pd.read_csv(file, names=["user", "item"], usecols=[1, 3], header=0, engine="python")
    if name == "TaFeng":
        with zipfile.ZipFile(f"data/{name}/archive.zip") as zfile:
            with zfile.open("ta_feng_all_months_merged.csv") as file:
                return pd.read_csv(file, names=["user", "item"], usecols=[1, 5], header=0, engine="python")
    if name == "Twitch100k":
        return pd.read_csv(f"data/{name}/100k_a.csv", names=["user", "item"], usecols=[0, 2], engine="python")
    if name == "Yelp":
        with tarfile.open(f"data/{name}/yelp_dataset.tar") as zfile:
            with zfile.extractfile("yelp_academic_dataset_review.json") as file:
                df = pd.read_json(file, lines=True)
                df.rename(columns={"user_id": "user", "business_id": "item", "stars": "rating"}, inplace=True)
                df = df[["user", "item", "rating"]]
                return df
    return 0


def cleanData(data: pd.DataFrame, name: str):
    # drop nan
    data.dropna(how="any")

    # make data implicit
    if name == "AliEC":
        data = data[data["rating"] == 1][["user", "item"]]
    if name == "Amazon_All_Beauty":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Appliances":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Arts_Crafts_and_Sewing":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Automotive":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Books":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_CDs_and_Vinyl":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Cell_Phones_and_Accessories":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Clothing_Shoes_and_Jewelry":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Digital_Music":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Electronics":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Fashion":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Gift_Cards":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Grocery_and_Gourmet_Food":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Home_and_Kitchen":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Industrial_and_Scientific":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Kindle_Store":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Luxury_Beauty":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Magazine_Subscriptions":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Movies_and_TV":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Musical_Instruments":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Office_Products":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Patio_Lawn_and_Garden":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Pet_Supplies":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Prime_Pantry":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Software":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Sports_and_Outdoors":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Tools_and_Home_Improvement":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Toys_and_Games":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Amazon_Video_Games":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Anime":
        data = data[(data["rating"] > 6) | (data["rating"] == -1)][["user", "item"]]
    if name == "BeerAdvocate":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "BookCrossing":
        data = data[data["rating"] > 6][["user", "item"]]
    if name == "CiaoDVD":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "DoubanBook":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "DoubanMovie":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "DoubanMusic":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "DoubanShort":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Epinions":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "FilmTrust":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Food":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "GoodReadsComics":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "GoogleLocalAlaska":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "GoogleLocalDelaware":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "GoogleLocalDistrictOfColumbia":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "GoogleLocalMontana":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "GoogleLocalVermont":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "Jester":
        data = data[(data["rating"] > 0) & (data["rating"] != 99)][["user", "item"]]
    if name == "LearningFromSets":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "LibraryThing":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "MarketBiasElectronics":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "MarketBiasModcloth":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "ModCloth":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "MovieLens1m":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "MovieLens100k":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "MovieLensLatestSmall":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "RentTheRunway":
        data = data[data["rating"] > 6][["user", "item"]]
    if name == "MovieTweetings":
        data = data[data["rating"] > 6][["user", "item"]]
    if name == "Netflix":
        data = data[data["rating"] > 3][["user", "item"]]
    if name == "RateBeer":
        data = data[data["rating"] > 12][["user", "item"]]
    if name == "Rekko":
        data = data[data["rating"] > 6][["user", "item"]]
    if name == "Yelp":
        data = data[data["rating"] > 3][["user", "item"]]

    # drop duplicates
    data.drop_duplicates(subset=["user", "item"], keep="last", inplace=True)
    return data

def pruneData(data: pd.DataFrame):
    u_cnt, i_cnt = Counter(data["user"]), Counter(data["item"])
    while min(u_cnt.values()) < 5 or min(i_cnt.values()) < 5:
        u_sig = [k for k in u_cnt if (u_cnt[k] >= 5)]
        i_sig = [k for k in i_cnt if (i_cnt[k] >= 5)]
        data = data[data["user"].isin(u_sig)]
        data = data[data["item"].isin(i_sig)]
        u_cnt, i_cnt = Counter(data["user"]), Counter(data["item"])
        if sum(u_cnt.values()) == 0 or sum(i_cnt.values()) == 0:
            print("Dataset empty after pruning.")
            break
    return data

def normalizeData(data: pd.DataFrame):
    for col in ["user", "item"]:
        unique_ids = {key: value for value, key in enumerate(data[col].unique())}
        data[col].update(data[col].map(unique_ids))
    return data

def generateInter(data: pd.DataFrame, name: str):
    path = f"data/{name}"
    if not os.path.exists(path):
            os.mkdir(path)
    data.to_csv(f"{path}/{name}.inter", sep="\t", index=False, header=["user:token", "item:token"])
    return

def preprocessData(dataset: str):
    if os.path.isfile(f"data/{dataset}/{dataset}.inter"):
        print(f"atomic file for {dataset} already generated.")
        return
    data = readData(dataset)
    data = cleanData(data, dataset)
    data = pruneData(data)
    data = normalizeData(data)
    generateInter(data, dataset)
    print(f"atomic file for {dataset} generated.")

if __name__ == "__main__":
    settings = json.load(open(f"./settings.json"))

    parser = argparse.ArgumentParser("Preprocess Data")
    parser.add_argument("--dataset", dest="dataset", type=str, required=True)
    args = parser.parse_args()
    dataset = args.dataset
    # for dataset in settings["datasets"]:
    preprocessData(dataset)