# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    extract_train_data.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/17 17:24:43 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm
import unicodedata

def normalize_lower(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode().lower()

    
train_dir = "disk/train-08/"

election_files = set([
    "Biden"
    "Trump",
    "Joe Biden",
    "JoeBiden",
    "Donald Trump",
    "realDonaldTrump"
    "Trump OR Biden",
    "biden OR joebiden",
    "trump OR donaldtrump OR realdonaldtrump",
])

months = set([
    "202001",
    "202002",
    "202003",
    "202004",
    "202005",
    "202006",
    # "202007",
])


# PB BS EW JB OT=others
def read_classified_hashtags():
    # labels = "PB BS EW JB OT".split()
    # classified_hts = {
    #     "PB": set(),
    #     "BS": set(),
    #     "EW": set(),
    #     "JB": set(),
    #     "OT": set(),
    # }

    # 2020-01-21
    # classified_hts = {
    #     "PB": set(),
    #     "BS": set(),
    #     "EW": set(),
    #     "JB": set(),
    #     "OT": set(),
    #     "MB": set()
    # }

    # 2020-03-06
    #classified_hts = {
    #    "BS": set(),
    #    "JB": set(),
    #    "OT": set(),
    #}
    
    # 2020-03-25
    # classified_hts = {
    #     "BS": set(),
    #     "JB": set(),
    # }

    # classified_hts = {
    #     "DT": set(),
    #     "BS": set(),
    #     "JB": set(),
    #     "OT": set(),
    # }

    classified_hts = {
        "AMLO": set(),
        "anti-AMLO": set()
    }
    # category_hts = {
    #     "JB": set(),
    #     "DT": set()
    # }

    # for line in open(train_dir + "hashtags.txt"):     # 2020-03-06
    #     if not line.startswith("#"):
    #         w = line.strip().split()
    #         _ht, label, category = w[0], w[1], w[2]
    #         if label == "UNK":
    #             continue
    #         # print(_ht, label)
    #         if label in classified_hts:
    #             classified_hts[label].add(_ht)
    #             category_hts[category].add(_ht)
                
    # print(classified_hts)
    # return classified_hts, category_hts

    for line in open(train_dir + "hashtags.txt"):     # 2020-03-06
        if not line.startswith("#"):
            w = line.strip().split()
            _ht, label = w[0], w[1]
            if label == "UNK":
                continue
            # print(_ht, label)
            if label in classified_hts:
                classified_hts[label].add(_ht)
                
    print(classified_hts)
    return classified_hts

# with open(train_dir + "train.txt", "w") as f: # 2020-03-06
#     for dt_dir in Path("raw_data").iterdir():
#         set_id = set() # remove dups
#         for in_name in dt_dir.iterdir():
#             if in_name.stem.split("-")[-1] in election_files and in_name.parts[1] in months:
#                 print(in_name)
#                 for line in tqdm(open(in_name)):
#                     label_bingo_times = 0
#                     label = None
#                     data = json.loads(line.strip())
                    
#                     # ignoring retweets
#                     if 'retweeted_status' in data and data["text"].startswith("RT @"): 
#                         continue
#                     set_hts = set([ht["text"].lower() for ht in data["hashtags"]])
#                     if not set_hts:
#                         continue
                        
#                     if data["id"] in set_id:
#                         continue
#                     set_id.add(data["id"])
                    
#                     for _label, _set_hts in classified_hts.items():
#                         for _ht in set_hts:
#                             if _ht in _set_hts:
#                                 label = _label
#                                 label_bingo_times += 1
#                                 break
                                
#                     # one tweet (in traindata) should have 0 or 1 class hashtag
#                     if label and label_bingo_times == 1:
#                         text = data["text"].replace("\n", " ").replace("\t", " ")
#                         f.write(label + "\t" + text + "\n")


if __name__ == "__main__":
    classified_hts = read_classified_hashtags()
    AMLO_hts = classified_hts["AMLO"]
    anti_AMLO_hts = classified_hts["anti-AMLO"]

    with open(train_dir + "train.txt", "w") as f:
        set_id = set()  # remove dups
        months = ["202008"]
        for month in months:
            for in_name in Path("raw_data" / month).rglob("*.txt"):
                print(in_name)
                for line in open(in_name):
                    try:
                        data = json.loads(line.strip())
                    except Exception:
                        print('json.loads Error:', line)
                        continue

                    label_bingo_times = 0
                    label = None
                    
                    # ignoring retweets
                    if 'retweeted_status' in data and data["text"].startswith("RT @"): 
                        continue
                    set_hts = set([normalize_lower(ht["text"]) for ht in data["hashtags"]])
                    if not set_hts:
                        continue
                        
                    if data["id"] in set_id:
                        continue
                    set_id.add(data["id"])

                    for _ht in set_hts:
                        if _ht in AMLO_hts:
                            label_bingo_times += 1
                            label = "AMLO"
                            break
                    for _ht in set_hts:
                        if _ht in anti_AMLO_hts:
                            label_bingo_times += 1
                            label = "anti-AMLO"
                            break

                    if not (label and label_bingo_times == 1):
                        continue
                        
                    # one tweet (in traindata) should have 0 or 1 class hashtag
                    if label and label_bingo_times == 1:
                        text = data["text"].replace("\n", " ").replace("\t", " ")
                        f.write(label + "\t" + text + "\n")
