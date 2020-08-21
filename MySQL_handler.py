# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    MySQL_handler.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:40:05 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/21 22:03:05 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from operator import index
import os
import unicodedata

import pendulum
from bs4 import BeautifulSoup
from sqlalchemy import (Column, DateTime, Float, Integer, String, Text, and_,
                        create_engine, desc, exists, or_, text)
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import query_expression, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import text
from sqlalchemy.sql.sqltypes import FLOAT
from tqdm import tqdm
import sqlite3

Base = declarative_base()


class Query(Base):
    __tablename__ = "query"
    word = Column(VARCHAR(255), primary_key=True)
    start_dt = Column(DATETIME)
    update_dt = Column(DATETIME)
    since_id = Column(INTEGER)
    category = Column(VARCHAR(255))
    subcategory = Column(VARCHAR(255))


class Query_Freq(Base):
    __tablename__ = "query_freq"
    query = Column(VARCHAR(255), primary_key=True)
    dt = Column(DATETIME)
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
    tweet_id = Column(INTEGER, primary_key=True)
    dt = Column(DATETIME, index=True)
    user_id = Column(INTEGER)
    source = Column(VARCHAR(100))
    amlo = Column(FLOAT)
    sentiment = Column(FLOAT)


class User(Base):
    __tablename__ = "user"
    user_id = Column(INTEGER, primary_key=True)
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
        except:
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


if __name__ == "__main__":
    init_db()

