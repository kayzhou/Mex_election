# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_run.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/08/23 23:43:19 by Zhenkun           #+#    #+#              #
#    Updated: 2020/08/31 21:26:05 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from MySQL_handler import *
from my_weapon import *


if __name__ == "__main__":
    # start = pendulum.datetime(2020, 8, 1, tz="UTC")
    start = pendulum.datetime(2020, 8, 20, tz="UTC")
    end = pendulum.datetime(2020, 8, 30, tz="UTC")

    for dt in pendulum.Period(start, end):
        print(dt)
        insert_all_query_freq(dt)
    
    # tweets_to_db(dt, dt.add(days=1))