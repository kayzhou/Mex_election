# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    MySQL_handler.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:40:05 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/21 18:35:10 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import os
import unicodedata

import pendulum
from bs4 import BeautifulSoup
from sqlalchemy import (Column, DateTime, Float, Integer, String, Text, and_,
                        create_engine, desc, exists, or_, text)
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import query_expression, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import text
from tqdm import tqdm
import sqlite3

Base = declarative_base()


class Query(Base):
    __tablename__ = "query"
    word = Column(VARCHAR(256), primary_key=True)
    start_dt = Column(DATETIME)
    update_dt = Column(DATETIME)
    since_id = Column(INTEGER)


class Query_Freq(Base):
    __tablename__ = "query_freq"
    query = Column(VARCHAR(256), primary_key=True)
    dt = Column(DATETIME)
    cnt = Column(INTEGER)


class Hashtag(Base):
    __tablename__ = "hashtag"
    query = Column(VARCHAR(256), primary_key=True)
    start_dt = Column(DATETIME, primary_key=True)
    end_dt = Column(DATETIME, primary_key=True)
    cnt = Column(INTEGER)
    category = Column(VARCHAR(256))


class Results_Pred(Base):
    __tablename__ = "results_pred"
    dt = Column(DATETIME, primary_key=True)
    category = Column(VARCHAR(256), primary_key=True)
    location = Column(VARCHAR(256), primary_key=True)
    rst = Column(VARCHAR(256))


class Results_Sent(Base):
    __tablename__ = "results_sent"
    dt = Column(DATETIME, primary_key=True)
    category = Column(VARCHAR(256), primary_key=True)
    location = Column(VARCHAR(256), primary_key=True)
    rst = Column(VARCHAR(256))


class Topic(Base):
    __tablename__ = "topic"
    category = Column(VARCHAR(256), primary_key=True)
    start_dt = Column(DATETIME, primary_key=True)
    end_dt = Column(DATETIME, primary_key=True)
    rst = Column(VARCHAR)


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


def get_keywords():
    """from tweets.db

    Returns:
        [type]: [description]
    """
    conn = sqlite3.connect("tweets.db")
    c = conn.cursor()
    # c.execute("SELECT * from keyword where bingo=1")
    c.execute("SELECT * from keyword")
    d = c.fetchall()
    conn.close()
    return d


if __name__ == "__main__":
    # init_db()
    queries = get_keywords()
    sess = get_session()
    for q in queries:
        _q = Query(
            word=q[1],
            start_dt=pendulum.Date(2020, 8, 1),
            update_dt=q[2],
            since_id=q[0]
        )
        sess.add(_q)
        sess.commit()
    sess.close()

