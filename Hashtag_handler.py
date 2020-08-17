# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    Hashtag_handler.py                                 :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:40:05 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/17 17:18:59 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import json
import pendulum
from sqlalchemy import (Column, DateTime, Float, Integer, String, Text, and_,
                        create_engine, desc, exists, or_, text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from pathlib import Path

from sqlalchemy.sql.expression import label

Base = declarative_base()


class Hashtag_Labelled(Base):
    __tablename__ = "hashtag_labelled"
    hashtag = Column(String, primary_key=True)
    created_at = Column(DateTime)
    label = Column(String)


class Hashtag_Weekly(Base):
    __tablename__ = "hashtag_Weekly"
    hashtag = Column(String, primary_key=True)
    created_at = Column(DateTime)
    count = Column(Integer)


class Hashtag_Labelled_Mex(Base):
    __tablename__ = "hashtag_labelled_mex"
    hashtag = Column(String, primary_key=True)
    created_at = Column(DateTime)
    label = Column(String)


class Hashtag_Weekly_Mex(Base):
    __tablename__ = "hashtag_weekly_mex"
    hashtag = Column(String, primary_key=True)
    created_at = Column(DateTime)
    count = Column(Integer)


# class Hashtag_All(Base):
#     __tablename__ = "hashtag_All"
#     hashtag = Column(String, primary_key=True)
#     updated_at = Column(DateTime)
#     label = Column(String)


###################### hashtags ######################
# def tweets_db_to_hashtags(sess, start, end):
#     """
#     One month
#     """
#     from collections import defaultdict
#     _hashtags = defaultdict(int)
#     _ht_M = defaultdict(int)
#     _ht_K = defaultdict(int)

#     tweets = sess.query(Tweet.hashtags).filter(
#         Tweet.source.is_(None),
#         Tweet.hashtags.isnot(None),
#         Tweet.proM > 0.75,
#         Tweet.dt >= start,
#         Tweet.dt < end).yield_per(5000)

#     for t in tqdm(tweets):
#         hts = t[0].split(",")
#         for ht in hts:
#             _hashtags[ht] += 1
#             _ht_M[ht] += 1

#     tweets = sess.query(Tweet.hashtags).filter(
#         Tweet.source.is_(None),
#         Tweet.hashtags.isnot(None),
#         Tweet.dt >= start,
#         Tweet.proK > 0.75,
#         Tweet.dt < end).yield_per(5000)

#     for t in tqdm(tweets):
#         hts = t[0].split(",")
#         for ht in hts:
#             _hashtags[ht] += 1
#             _ht_K[ht] += 1

#     end = pendulum.today()
#     _hashtags = [Hashtag(hashtag=ht, update_dt=end, count=cnt, M_count=_ht_M[ht], K_count=_ht_K[ht])
#                  for ht, cnt in _hashtags.items()]
#     print(len(_hashtags))

#     sess.query(Hashtag).delete()
#     sess.commit()
#     sess.add_all(_hashtags)
#     sess.commit()


# def get_top_hashtags(sess):
#     from collections import Counter
#     _hashtags = Counter()

#     tweets = sess.query(Tweet.hashtags).filter(
#         Tweet.source.is_(None),
#         Tweet.hashtags.isnot(None),
#         Tweet.dt >= "2019-04-10",
#         Tweet.dt < "2019-05-24").yield_per(5000)

#     for t in tqdm(tweets):
#         hts = t[0].split(",")
#         for ht in hts:
#             _hashtags[ht] += 1

#     print(_hashtags.most_common(200))


# def tweets_db_to_hashtags75(sess, end):
#     """
#     all tweets
#     """
#     from collections import defaultdict
#     _hashtags = defaultdict(int)

#     tweets = sess.query(Tweet.hashtags).filter(
#         Tweet.source.is_(None),
#         Tweet.hashtags.isnot(None),
#         Tweet.dt < end,
#         or_(Tweet.proM > 0.75, Tweet.proK > 0.75)).yield_per(5000)

#     for t in tqdm(tweets):
#         hts = t[0].split(",")
#         for ht in hts:
#             _hashtags[ht] += 1

#     _hashtags = [Hashtag75(hashtag=ht, update_dt=end, count=cnt)
#                  for ht, cnt in _hashtags.items()]
#     print(len(_hashtags))

#     sess.query(Hashtag75).delete()
#     sess.commit()

#     sess.add_all(_hashtags)
#     sess.commit()


# def count_of_hashtags(sess, start, end):
#     from collections import defaultdict
#     _hashtags = defaultdict(int)
#     period = pendulum.period(start, end)

#     for dt in period:
#         tweets = get_tweets_day_with_hashtags(sess, dt)
#         for t in tqdm(tweets):
#             if t.proM > 0.75 or t.proM < 0.25:
#                 for ht in t.hashtags.split(","):
#                     _hashtags[ht] += 1
#     print(_hashtags)
#     return dict(_hashtags)


def add_camp_hashtags_from_file(in_name, clear=False):
    sess = get_session()

    if clear:
        sess.query(Hashtag_Labelled).delete()
        sess.commit()

    # "data/hashtags/hashtags-democrats-20200121-v2.txt"
    for line in open(in_name):
        if not line.startswith("#"):
            w = line.strip().split()
            if len(w) == 3 and w[0] in ["JB", "BS"]:
                ht, label = w[1], w[0]
                print(label, ht)
                sess.add(Hashtag_Labelled(
                    hashtag=ht,
                    created_at=pendulum.datetime(2020, 2, 6),
                    label=label
                ))
    sess.commit()
    sess.close()


def add_camp_hashtags_from_dict(hts_camp):
    sess = get_session()
    if clear:
        sess.query(Hashtag_Labelled).delete()
        sess.commit()

    # "data/hashtags/hashtags-democrats-20200121-v2.txt"
    for ht, camp in hts_camp:
        sess.add(Hashtag_Labelled(
            hashtag=ht,
            created_at=pendulum.today(),
            label=camp
        ))
    sess.commit()
    sess.close()


def add_weekly_hashtags_from_file(in_name, clear=True):
    sess = get_session()

    if clear:
        sess.query(Hashtag_Weekly).delete()
        sess.commit()

    for line in open(in_name):
        if not line.startswith("#"):
            w = line.strip().split()
            sess.add(Hashtag_Weekly(
                hashtag=w[0],
                created_at=pendulum.datetime(2020, 3, 17),
                count=w[1]
            ))
    sess.commit()
    sess.close()


def add_weekly_hashtags_from_file_mex(in_name, clear=True):
    sess = get_session()

    if clear:
        sess.query(Hashtag_Weekly_Mex).delete()
        sess.commit()

    i = 0
    for line in open(in_name):
        if not line.startswith("#"):
            i += 1
            w = line.strip().split()
            sess.add(Hashtag_Weekly_Mex(
                hashtag=w[0],
                created_at=pendulum.datetime(2020, 8, 1),
                count=w[1]
            ))
            if i >= 300:
                break
    sess.commit()
    sess.close()


def add_camp_hashtags_from_json(in_name, clear=False):
    sess = get_session()

    if clear:
        sess.query(Hashtag_Labelled).delete()
        sess.commit()

    data = json.load(open(in_name))
    for d in data:
        sess.add(Hashtag_Labelled(
            hashtag=d[0],
            created_at=pendulum.today(),
            label=d[1]
        ))
    sess.commit()
    sess.close()


def add_camp_hashtags_from_json_mex(in_name, clear=False):
    sess = get_session()

    if clear:
        sess.query(Hashtag_Labelled_Mex).delete()
        sess.commit()

    data = json.load(open(in_name))
    
    today = pendulum.today()

    for d in data:
        print(d)
        if d[1] == "unk":
            d[1] = "UNK"
        elif d[1] == "amlo":
            d[1] = "AMLO"

        existing = sess.query(Hashtag_Labelled_Mex).filter(
            Hashtag_Labelled_Mex.hashtag==d[0]).first()
        if existing:
            sess.query(Hashtag_Labelled_Mex).filter(
                Hashtag_Labelled_Mex.hashtag==d[0]).update(
                    {"created_at": today, "label": d[1]
            })
        else:
            sess.add(Hashtag_Labelled_Mex(
                hashtag=d[0],
                created_at=today,
                label=d[1]
            ))
    sess.commit()
    sess.close()


def get_camp_hashtags():
    print("Loaded camp hashtags.")
    sess = get_session()
    hts = sess.query(Hashtag_Labelled)
    hts = [(ht.hashtag, ht.created_at, ht.label) for ht in hts]
    print(f"Loaded {len(hts)} camp hashtags.")
    sess.close()
    return hts


def get_last_hashtags(num=500):
    print("Loaded last (week) hashtags.")
    sess = get_session()
    hts = sess.query(
        Hashtag_Weekly.hashtag, Hashtag_Weekly.created_at, Hashtag_Labelled.label, Hashtag_Weekly.count).outerjoin(Hashtag_Labelled, Hashtag_Weekly.hashtag==Hashtag_Labelled.hashtag).order_by(
        Hashtag_Weekly.count.desc()).limit(num).all()
    sess.close()
    return hts


def get_camp_hashtags_mex():
    print("Loaded camp hashtags.")
    sess = get_session()
    hts = sess.query(Hashtag_Labelled_Mex)
    hts = [(ht.hashtag, ht.created_at, ht.label) for ht in hts]
    print(f"Loaded {len(hts)} camp hashtags.")
    sess.close()
    return hts


def get_last_hashtags_mex(num=300):
    print("Loaded last (week) hashtags.")
    sess = get_session()
    hts = sess.query(
        Hashtag_Weekly_Mex.hashtag, 
        Hashtag_Weekly_Mex.created_at, 
        Hashtag_Labelled_Mex.label,
        Hashtag_Weekly_Mex.count).outerjoin(Hashtag_Labelled_Mex,
        Hashtag_Weekly_Mex.hashtag==Hashtag_Labelled_Mex.hashtag).order_by(
        Hashtag_Weekly_Mex.count.desc()).limit(num).all()
    sess.close()
    return hts


################## get section ##################
def get_session():
    engine = create_engine(
        "sqlite:////home/alex/kayzhou/US_election/data/hashtag.db")
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session


def init_db():
    engine = create_engine(
        "sqlite:////home/alex/kayzhou/US_election/data/hashtag.db")
    Base.metadata.create_all(engine)


### For Web ###
def get_US_hts_for_web(num):
    # list: [ [ hashtag, created_at, camp ] ]
    hts_camp = get_camp_hashtags()

    # list: [ [ hashtag, created_at, count, camp ] ]
    hts_weekly = get_last_hashtags(num)
    print(hts_weekly)
    update_dt = hts_weekly[0][2]
    hts_weekly = [[ht[0], ht[2], ht[3]] for ht in hts_weekly]

    return hts_camp, hts_weekly


### For Web ###
def get_MEX_hts_for_web(num):
    # list: [ [ hashtag, created_at, camp ] ]
    hts_camp = get_camp_hashtags_mex()

    # list: [ [ hashtag, created_at, count, camp ] ]
    hts_weekly = get_last_hashtags_mex(num)
    print(hts_weekly)
    update_dt = hts_weekly[0][2]
    hts_weekly = [[ht[0], ht[2], ht[3]] for ht in hts_weekly]

    return hts_camp, hts_weekly


if __name__ == "__main__":
    init_db()

    # 导入
    # path = "/media/zhenkun/Argentina_election/web/data/submit"
    # for in_file in Path(path).rglob("*.json"):
    #     print(in_file)
    #     if str(in_file) > "/media/zhenkun/Argentina_election/web/data/submit/2020-08-11 16":
    #         add_camp_hashtags_from_json_mex(in_file)

    # add_weekly_hashtags_from_file_mex("/home/alex/kayzhou/US_election/data/hashtags-MEX-20200811.txt")

    from extract_train_data import normalize_lower
    # 导出已经标注的hashtag
    set_hts = set()
    hashtags = get_camp_hashtags_mex()
    with open("disk/train-08/hashtags.txt", "w") as f:
        for ht in hashtags:
            ht_name = normalize_lower(ht[0])
            if ht_name in set_hts:
                continue
            set_hts.add(ht_name)
            f.write(f"{ht_name} {ht[2]}\n")