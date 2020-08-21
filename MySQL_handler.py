# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    MySQL_handler.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:40:05 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/21 18:21:37 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import os
import unicodedata

import pendulum
from bs4 import BeautifulSoup
from sqlalchemy import (Column, DateTime, Float, Integer, String, Text, and_,
                        create_engine, desc, exists, or_, text)
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from tqdm import tqdm


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
    __tablename__ = ""
    category = Column(VARCHAR(256), primary_key=True)
    start_dt = Column(DATETIME, primary_key=True)
    end_dt = Column(DATETIME, primary_key=True)
    rst = Column(VARCHAR)


def get_session():
    engine = create_engine("mysql+pymysql://kcore:kcore123.@localhost:3306/mex")
    session = sessionmaker(bind=engine)()
    return session
    

def init_db():
    engine = create_engine("mysql+pymysql://kcore:kcore123.@localhost:3306/mex")
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    init_db()
