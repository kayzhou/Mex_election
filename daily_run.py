# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_run.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/08/23 23:43:19 by Zhenkun           #+#    #+#              #
#    Updated: 2020/08/24 00:00:25 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from MySQL_handler import *
from my_weapon import *

start = pendulum.date(2020, 8, 1)
end = pendulum.date(2020, 8, 21)

for dt in pendulum.Period(start, end):
    upsert_all_query_freq(dt)