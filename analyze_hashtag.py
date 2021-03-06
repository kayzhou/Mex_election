# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_hashtag.py                                 :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/17 18:06:05 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm


demo_files = set([
    "Joe Biden",
    "JoeBiden",
    "Bernie Sanders",
    "SenSanders",
    # "Michael Bennet",
    # "SenatorBennet",
    # "Mike Bloomberg",
    # "MikeBloomberg",
    # "Pete Buttigieg",
    # "PeteButtigieg",
    # "John Delaney",
    # "JohnDelaney",
    # "Tulsi Gabbard",
    # "TulsiGabbard",
    # "Amy Klobuchar",
    # "amyklobuchar",
    # "Deval Patrick",
    # "DevalPatrick",
    # "Tom Steyer",
    # "TomSteyer",
    # "Elizabeth Warren",
    # "ewarren",
    # "Andrew Yang",
    # "AndrewYang",
])

trump_files = [
    "trump",
    "biden",
]
 
months = set([
    "202006",
    "202005",
    "202004",
    "202003",
])


def write_top_hashtags(in_files, out_name):
    all_hts = Counter()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in in_files and in_name.parts[1] in months:
            print(in_name)
            for line in tqdm(open(in_name)):
                hts = json.loads(line)["hashtags"]
                for ht in hts:
                    all_hts[ht["text"].lower()] += 1

    with open(out_name, "w") as f:
        for ht, cnt in all_hts.most_common(1000):
            print(ht, cnt, file=f)


def write_top_hashtags_mex(out_name):
    all_hts = Counter()
    file_names = sorted(Path("/media/wangjiannan/Mexico_election_raw_tweets/202008").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        print(in_name)
        if str(in_name).endswith("OR.txt"):
            continue
        for line in tqdm(open(in_name)):
            try:
                hts = json.loads(line)["hashtags"]
            except:
                print("json.loads() Error.")
                continue
            for ht in hts:
                all_hts[ht["text"].lower()] += 1

    with open(out_name, "w") as f:
        for ht, cnt in all_hts.most_common(5000):
            print(ht, cnt, file=f)

def write_top_words_mex(out_name):
    all_hts = Counter()
    file_names = sorted(Path("raw_data/202008").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        print(in_name)
        if str(in_name).endswith("OR.txt"):
            continue
        for line in tqdm(open(in_name)):
            try:
                text = json.loads(line)["text"]
            except:
                print("json.loads() Error.")
                continue
            for w in text.split():
                all_hts[w.lower()] += 1

    with open(out_name, "w") as f:
        for ht, cnt in all_hts.most_common(1000):
            print(ht, cnt, file=f)


def write_top_trump_biden_hashtags(out_name):
    all_hts = Counter()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        name = in_name.stem.split("-")[-1].lower()
        if "trump" in name or "biden" in name:
            if in_name.parts[1] in months:
                print(in_name)
                for line in tqdm(open(in_name)):
                    hts = json.loads(line)["hashtags"]
                    for ht in hts:
                        all_hts[ht["text"].lower()] += 1


    with open(out_name, "w") as f:
        for ht, cnt in all_hts.most_common(1000):
            f.write(f"{ht},{cnt}\n")


def write_cooccurrence_hashtags(out_name):
    from extract_train_data import normalize_lower
    with open(out_name, "w") as f:
        for in_name in Path("raw_data/202008").rglob("*.txt"):
            print(in_name)
            for line in open(in_name):
                hts = json.loads(line)["hashtags"]
                if hts and len(hts) >= 1:
                    f.write(" ".join([normalize_lower(ht["text"]) for ht in hts]) + "\n")
                    

def get_hts(in_name):
    hts = {}
    for line in open(in_name):
        if not line.startswith("#"):
            w = line.strip().split()
            if len(w) == 3:
                hts[w[1]] = w[0]
    print(hts)
    return hts


def label_based_on_before(in_name, out_name):
    hts = get_hts("data/hashtags-20200201_classified_hernan_Feb6.txt")
    with open(out_name, "w") as f:
        for line in open(in_name):
            w = line.strip().split()
            if w[0] in hts:
                f.write(f"{hts[w[0]]} {w[0]} {w[1]}\n")
            else:
                f.write(f"{w[0]} {w[1]}\n")
            

if __name__ == "__main__":
    # write_top_hashtags(demo_files, "hashtags-democrats-20200305.txt")
    # write_top_trump_biden_hashtags("data/hashtags-democrats-20200305.txt")
    # write_top_hashtags_mex("hashtags-MEX-20200811.txt")
    # write_top_words_mex("data/words-MEX-20200811.txt")


    # write_top_hashtags(trump_files, "hashtags-trump-20200318.txt")
    # label_based_on_before("data/hashtags-democrats-20200121.txt", "data/hashtags-democrats-20200121-v2.txt")
    # label_based_on_before("data/hashtags-trump-20200121.txt", "data/hashtags-trump-20200121-v2.txt")

    # write_cooccurrence_hashtags(trump_files, "hashtags-co-20200301-20200625.txt")
    write_cooccurrence_hashtags("data/hashtags-co-20200801-20200817.txt")
