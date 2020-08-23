# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_run.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/08/23 23:43:19 by Zhenkun           #+#    #+#              #
#    Updated: 2020/08/24 00:03:21 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from MySQL_handler import *
from my_weapon import *


if __name__ == "__main__":
    start = pendulum.datetime(2020, 8, 1, tz="UTC")
    end = pendulum.datetime(2020, 8, 21, tz="UTC")

    for dt in pendulum.Period(start, end):
        upsert_all_query_freq(dt)