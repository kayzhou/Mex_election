# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    my_topic.py                                        :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/14 11:08:14 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/23 22:09:54 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import json
import pprint
import re
import string
from collections import Counter
from pathlib import Path
from pprint import pprint

import matplotlib
import scipy
# spacy for lemmatization
import spacy
from nltk.corpus import stopwords
from tqdm import tqdm

# Gensim
import gensim
import gensim.corpora as corpora
# from nltk.corpus import stopwords
# stop_words = stopwords.words('english')
from extract_train_data import normalize_lower
from gensim.corpora import Dictionary
from gensim.models import CoherenceModel, LdaModel
from gensim.test.utils import datapath
from gensim.utils import simple_preprocess

from my_weapon import *
from TwProcess import *

# matplotlib.rcParams["font.size"] = 14
# sns.set_style("darkgrid")
# ira_c = sns.color_palette("coolwarm", 8)[7]
# all_c = sns.color_palette("coolwarm", 8)[0]

# nlp = spacy.load('es', disable=['parser', 'ner'])

tokenizer = CustomTweetTokenizer(preserve_case=False,
                                 reduce_len=True,
                                 strip_handles=False,
                                 normalize_usernames=False,
                                 normalize_urls=True)

stop_words = json.load(open("data/spanish-stop-words.json"))["words"]
stop_words = [normalize_lower(w) for w in stop_words]
stop_words.extend([
    "rt", "…", "...", "URL", "http", "https", "“", "”", "‘", "’", "get", "2", "new", "one", "i'm", "make",
    "go", "good", "say", "says", "know", "day", "..", "take", "got", "1", "going", "4", "3", "two", "n",
    "like", "via", "u", "would", "still", "first", "that's", "look", "way", "last", "said", "let",
    "twitter", "ever", "always", "another", "many", "things", "may", "big", "come", "keep", "RT",
    "5", "time", "much", "_", "cound", "-", '"', "|"
])
stop_words.extend([',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$', '%'])
stop_words = set(stop_words)


class Topic(object):
    def __init__(self):
        print("LDA init.")

    def load_text(self):
        print("Loading ...")
        texts_out = []
        # out_file = open("data/LDA_corpus.txt", "w")
        # set_id = set()  # remove dups
        # months = ["202008"]
        # for month in months:
        #     for in_name in Path("raw_data/" + month).rglob("*.txt"):
        #         print(in_name)
        #         for line in open(in_name):
        #             try:
        #                 data = json.loads(line.strip())
        #             except Exception:
        #                 print('json.loads Error:', line)
        #                 continue
                        
        #             if data["id"] in set_id:
        #                 continue
        #             set_id.add(data["id"])
                    
        #             text = data["text"].replace("\n", " ").replace("\t", " ")
        #             words = tokenizer.tokenize(text)
        #             if words:
        #                 texts_out.append(words)
        #                 out_file.write(" ".join(words) + "\n")
        for line in open("data/LDA_corpus.txt"):
            words = [w for w in line.strip().split() if w not in stop_words and len(w) > 1]
            texts_out.append(words)

        # Create Dictionary
        self.id2word = corpora.Dictionary(texts_out)

        # Create Corpus
        self.texts = texts_out

        # Term Document Frequency
        self.corpus = [self.id2word.doc2bow(text) for text in texts_out]

    def run(self):
        for i in range(10):
            print(f"---------------------- {i} ----------------------")
            # Can take a long time to run.
            lda_model = gensim.models.ldamodel.LdaModel(corpus=self.corpus, id2word=self.id2word, num_topics=7, chunksize=1000)
            print(lda_model.print_topics())
            # Compute Perplexity
            print('Perplexity: ', lda_model.log_perplexity(self.corpus))  # a measure of how good the model is. lower the better.

            # Compute Coherence Score
            coherence_model_lda = CoherenceModel(model=lda_model, texts=self.texts, dictionary=self.id2word, coherence='c_v')
            coherence_lda = coherence_model_lda.get_coherence()
            print('Coherence Score: ', coherence_lda)
            
            lda_model.save(f"disk/model/LDA-{i}.mod")

    def load_model(self):
        self.lda_model = LdaModel.load("model/LDA-78.mod")

    # def lemmatization(self, sent, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV', 'PROPN']):
    #     """https://spacy.io/api/annotation"""
    #     sent = " ".join(sent)
    #     sent = re.sub(r'#(\w+)', r'itstopiczzz\1', sent)
    #     sent = re.sub(r'@(\w+)', r'itsmentionzzz\1', sent)
    #     doc = nlp(sent)
        
    #     _d = [token.lemma_ for token in doc if token.pos_ in allowed_postags and token.lemma_ not in stop_words and token.lemma_]
        
    #     _d = [x.replace('itstopiczzz', '#') for x in _d]
    #     _d = [x.replace('itsmentionzzz', '@') for x in _d]
    #     return _d

    def predict(self, text):
        text = text.replace("\n", " ").replace("\t", " ")
        words = tokenizer.tokenize(text)
        words = [w for w in words if w not in stop_words and w]
        # text = self.lemmatization(words)
        text = self.id2word.doc2bow(text)
        return self.lda_model.get_document_topics(text)


if __name__ == "__main__":
    Lebron = Topic()
    Lebron.load_text()
    Lebron.run()
