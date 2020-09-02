# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_run.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/08/23 23:43:19 by Zhenkun           #+#    #+#              #
#    Updated: 2020/09/02 22:14:29 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from MySQL_handler import *
from my_weapon import *


if __name__ == "__main__":

    end = pendulum.today(tz="UTC") # not include this date
    start = pendulum.yesterday(tz="UTC") # include this date

    # start = pendulum.datetime(2020, 8, 20, tz="UTC")
    # end = pendulum.datetime(2020, 9, 2, tz="UTC")
    # for dt in pendulum.Period(start, end):
    #     print(dt)
    #     insert_all_query_freq(dt)

    insert_all_query_freq(start)
    tweets_to_db(start, end, clear=True)
    users_to_db(start, end)