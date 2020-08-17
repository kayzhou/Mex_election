# -*- coding: utf-8 -*-
# Author: Kay Zhou
# Date: 2019-03-30 19:19:22

import joblib
from nltk import ngrams
import sklearn

from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import SelectFromModel
# from sklearn.grid_search import GridSearchCV
from sklearn.model_selection import cross_val_score, train_test_split

from imblearn.over_sampling import ADASYN, SMOTE, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler


# Multinomial Naive Bayes Classifier
def naive_bayes_classifier(train_x, train_y):
    model = sklearn.naive_bayes.MultinomialNB(alpha=0.01)
    model.fit(train_x, train_y)
    return model


# KNN Classifier
def knn_classifier(train_x, train_y):
    model = sklearn.neighbors.KNeighborsClassifier()
    model.fit(train_x, train_y)
    return model


# Logistic Regression Classifier
def logistic_regression_classifier(train_x, train_y):
    model = sklearn.linear_model.LogisticRegression(penalty='l2')
    model.fit(train_x, train_y)
    return model


# Decision Tree Classifier
def decision_tree_classifier(train_x, train_y):
    model = sklearn.tree.DecisionTreeClassifier()
    model.fit(train_x, train_y)
    return model


# Random Forest Classifier
def random_forest_classifier(train_x, train_y):
    model = sklearn.ensemble.RandomForestClassifier(n_estimators=8)
    model.fit(train_x, train_y)
    return model


# GBDT(Gradient Boosting Decision Tree) Classifier
def gradient_boosting_classifier(train_x, train_y):
    model = sklearn.ensemble.GradientBoostingClassifier(n_estimators=200)
    model.fit(train_x, train_y)
    return model


# SVM Classifier
def svm_classifier(train_x, train_y):
    model = sklearn.svm.SVC(kernel='rbf', probability=True)
    model.fit(train_x, train_y)
    return model


# SVM Linear Classifier
def svm_linear_classifier(train_x, train_y):
    model = sklearn.svm.LinearSVC()
    model.fit(train_x, train_y)
    return model


# SVM Classifier using cross validation
# def svm_cross_validation(train_x, train_y):

#     model = SVC(kernel='rbf', probability=True)
#     param_grid = {'C': [1e-3, 1e-2, 1e-1, 1, 10,
#                         100, 1000], 'gamma': [0.001, 0.0001]}
#     grid_search = GridSearchCV(model, param_grid, n_jobs=1, verbose=1)
#     grid_search.fit(train_x, train_y)
#     best_parameters = grid_search.best_estimator_.get_params()
#     for para, val in list(best_parameters.items()):
#         print(para, val)
#     model = SVC(kernel='rbf', C=best_parameters['C'],
#                 gamma=best_parameters['gamma'], probability=True)
#     model.fit(train_x, train_y)
