In /home/ec2-user/zhenkun/Mex_election:
#1. Handling the mySQL database to connect AWS with Dashboard. 
MySQL_handler.py helps to import results to mysql database, including the query_freq, tweets and users.

#2. Obtaining Geolocalization/gender/age of users
collect_user.py
collect_location.py
collect_face.py

#3. Training machine learning model from hashtags to classify globally all tweets and post according to  AMLO anti-AMLO. 
train.py, TwProcess.py
classifier.py -> Camp_Classifier

#4. Sentiment analysis of tweets/post for local analysis of candidates at State/City level. 
classifier.py -> Senti_Classifier

#5. Performing LDA analysis on posts/tweets (nation, states, candicates)
my_topic.py and my_topic.ipynb (generate HTML code)

In /home/ec2-user/zhenkun/tweets-collection-Mexico-election
Mex Twitter collection

In /home/ec2-user/zhenkun/mex-website:
Website to show logs of Mex Twitter collection