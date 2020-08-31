# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    MySQL_handler.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:40:05 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/31 19:53:46 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import os
import unicodedata
from operator import index

import pendulum
from bs4 import BeautifulSoup
from sqlalchemy import (Column, DateTime, Float, Integer, String, Text, and_,
                        create_engine, desc, exists, or_, text)
from sqlalchemy.dialects.mysql import BIGINT, DATETIME, FLOAT, INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import query_expression, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import text
from sqlalchemy.sql.sqltypes import FLOAT
from sqlalchemy.sql import text
from tqdm import tqdm

from read_raw_data import read_historical_tweets_freq, read_historical_tweets, read_historical_tweets_freq_temp

Base = declarative_base()

official_twitter_clients = set([
    'Twitter for iPhone',
    'Twitter for Android',
    'Twitter Web Client',
    'Twitter Web App',
    'Twitter for iPad',
    'Mobile Web (M5)',
    'TweetDeck',
    'Mobile Web',
    'Mobile Web (M2)',
    'Twitter for Windows',
    'Twitter for Windows Phone',
    'Twitter for BlackBerry',
    'Twitter for Android Tablets',
    'Twitter for Mac',
    'Twitter for BlackBerry®',
    'Twitter Dashboard for iPhone',
    'Twitter for iPhone',
    'Twitter Ads',
    'Twitter for  Android',
    'Twitter for Apple Watch',
    'Twitter Business Experience',
    'Twitter for Google TV',
    'Chirp (Twitter Chrome extension)',
    'Twitter for Samsung Tablets',
    'Twitter for MediaTek Phones',
    'Google',
    'Facebook',
    'Twitter for Mac',
    'iOS',
    'Instagram',
    'Vine - Make a Scene',
    'Tumblr',
])


class Query(Base):
    __tablename__ = "query"
    word = Column(VARCHAR(255), primary_key=True)
    start_dt = Column(DATETIME)
    update_dt = Column(DATETIME)
    since_id = Column(BIGINT)
    category = Column(VARCHAR(255))
    subcategory = Column(VARCHAR(255))


class Query_Freq(Base):
    __tablename__ = "query_freq"
    query = Column(VARCHAR(255), primary_key=True)
    dt = Column(DATETIME, primary_key=True)
    cnt = Column(INTEGER)


class Hashtag(Base):
    __tablename__ = "hashtag"
    query = Column(VARCHAR(255), primary_key=True)
    start_dt = Column(DATETIME, primary_key=True)
    end_dt = Column(DATETIME, primary_key=True)
    cnt = Column(INTEGER)
    category = Column(VARCHAR(255))
    label = Column(VARCHAR(10))


class Results_Pred(Base):
    __tablename__ = "results_pred"
    dt = Column(DATETIME, primary_key=True)
    category = Column(VARCHAR(255), primary_key=True)
    location = Column(VARCHAR(255), primary_key=True)
    rst = Column(VARCHAR(255))


class Results_Sent(Base):
    __tablename__ = "results_sent"
    dt = Column(DATETIME, primary_key=True)
    category = Column(VARCHAR(255), primary_key=True)
    location = Column(VARCHAR(255), primary_key=True)
    rst = Column(VARCHAR(255))


class Topic(Base):
    __tablename__ = "topic"
    category = Column(VARCHAR(255), primary_key=True)
    start_dt = Column(DATETIME, primary_key=True)
    end_dt = Column(DATETIME, primary_key=True)
    rst = Column(VARCHAR)


class Tweet(Base):
    __tablename__ = "tweet"
    tweet_id = Column(BIGINT, primary_key=True)
    dt = Column(DATETIME, index=True)
    user_id = Column(BIGINT)
    source = Column(VARCHAR(100))
    amlo = Column(FLOAT)
    sentiment = Column(FLOAT)


class User(Base):
    __tablename__ = "user"
    user_id = Column(BIGINT, primary_key=True)
    location = Column(VARCHAR(255))
    state = Column(VARCHAR(255))
    gender = Column(VARCHAR(10))
    age = Column(VARCHAR(10))
    

def get_engine():
    engine = create_engine("mysql+pymysql://kcore:kcore123.@localhost:3306/mex")
    return engine


def get_session():
    engine = create_engine("mysql+pymysql://kcore:kcore123.@localhost:3306/mex")
    session = sessionmaker(bind=engine)()
    return session
    

def init_db():
    engine = create_engine("mysql+pymysql://kcore:kcore123.@localhost:3306/mex")
    Base.metadata.create_all(engine)


def get_query():
    engine = get_engine()
    with engine.connect() as con:
        rs = con.execute('SELECT * FROM query')
    return rs


def insert_query(word):
    engine = get_engine()
    with engine.connect() as con:
        try:
            print(word)
            con.execute(f"INSERT INTO query VALUES (word, start_dt, since_id) ('{word}', '2020-08-01 00:00:00', 1);")
        except Exception:
            print('"{}" have exists in the list.'.format(word))


def update_query(word, since_id):
    '''
    update keyword
    '''
    # print("update")
    update_dt = pendulum.now().to_datetime_string()
    engine = get_engine()
    with engine.connect() as con:
        con.execute(f"UPDATE query SET since_id={since_id}, update_dt='{update_dt}' WHERE word='{word}';")


def insert_all(in_name):
    for line in open(in_name):
        word = line.strip()
        if word and not word.startswith("#"):
            insert_query(word)
            

def insert_all_query_freq(dt):
    rsts = read_historical_tweets_freq(dt, dt.add(days=1))
    sess = get_session()
    dt_str = dt.to_datetime_string()
    for q in rsts:
        if not sess.query(exists().where(and_(Query_Freq.query == q, Query_Freq.dt == dt_str))).scalar():
            sess.add(Query_Freq(query=q, dt=dt_str, cnt=rsts[q]))
    sess.commit()
    sess.close()


def insert_all_query_freq_temp():
    # rsts = read_historical_tweets_freq_temp(pendulum.datetime(2020, 8, 20, tz="UTC"))
    sess = get_session()
    for q, rsts in read_historical_tweets_freq_temp(pendulum.datetime(2020, 8, 20, tz="UTC")):
        for dt in pendulum.period(pendulum.datetime(2020, 8, 1, tz="UTC"), pendulum.datetime(2020, 8, 19, tz="UTC")):
            dt = dt.to_date_string()
            if dt not in rsts:
                rsts[dt] = 0
            if not sess.query(exists().where(and_(Query_Freq.query == q, Query_Freq.dt == dt))).scalar():
                sess.add(Query_Freq(query=q, dt=dt, cnt=rsts[dt]))
            sess.commit()
    sess.close()


def get_source_text(_source):
    _sou = BeautifulSoup(_source, features="lxml").get_text()
    if _sou in official_twitter_clients:
        return None
    else:
        return _sou
        

def tweets_to_db(start, end, clear=False):
    """
    import tweets to database with prediction
    """
    sess = get_session()
    if clear:
        print("deleting >=", start, "<", end)
        sess.query(Tweet).filter(Tweet.dt >= start, Tweet.dt < end).delete()
        sess.commit()
    
    from classifier import Camp_Classifier
    Lebron = Camp_Classifier()
    Lebron.load()

    X = []
    tweets_data = []

    from read_raw_data import read_historical_tweets as read_tweets

    for d, dt in read_tweets(start, end):
        # print(d)
        tweet_id = d["id"]
        uid = d["user"]["id"]

        if 'source' in d:
            _sou = get_source_text(d["source"])
        else:
            _sou = "No source"

        tweets_data.append(
            Tweet(tweet_id=tweet_id,
                  user_id=uid,
                  dt=dt,
                  source=_sou)
        )
        X.append(d)
        
        if len(tweets_data) == 5000:
            json_rst = Lebron.predict(X)
            for i in range(len(tweets_data)):
                rst = json_rst[tweets_data[i].tweet_id]
                tweets_data[i].amlo = float(round(rst[1], 3))

            sess.add_all(tweets_data)
            sess.commit()
            X = []
            tweets_data = []

    if tweets_data:
        json_rst = Lebron.predict(X)
        for i in range(len(tweets_data)):
            rst = json_rst[tweets_data[i].tweet_id]
            tweets_data[i].amlo = float(round(rst[1], 3))

        sess.add_all(tweets_data)
        sess.commit()
    sess.close()
    

def users_to_db(end):
    """
    import tweets to database with prediction
    """
    sess = get_session()
    
    count = 0
    users_data = []
    from read_raw_data import read_historical_users_temp as read_users

    for u in read_users(end):
        if "location" in u:
            users_data.append(User(user_id=u['id'], location=u["location"]))
        else:
            users_data.append(User(user_id=u['id']))
        count += 1

        if count >= 5000:
            sess.add_all(users_data)
            sess.commit()
            users_data = []
            count = 0

    sess.add_all(users_data)
    sess.commit()
    sess.close()


if __name__ == "__main__":
    # init_db()
    # insert_all_query_freq_temp()
    end = pendulum.datetime(2020, 8, 30, tz="UTC")
    users_to_db(end)
