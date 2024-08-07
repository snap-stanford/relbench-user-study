# RelBench User Study

[![Relbench Website](https://img.shields.io/badge/website-live-brightgreen)](https://relbench.stanford.edu)
[![Twitter](https://img.shields.io/twitter/url/https/twitter.com/cloudposse.svg?style=social&label=Follow%20%40RelBench)](https://twitter.com/RelBench)

[**Website**](https://relbench.stanford.edu) | [**Relbench Repo**](https://github.com/snap-stanford/relbench) | [**Paper**](https://arxiv.org/abs/2407.20060) | [**Mailing List**](https://groups.google.com/forum/#!forum/relbench/join)

## Description

This repo hosts the code used in the RelBench User Study.  The goal of the study is to benchmark the
performance of a classical ML model (LightGBM) with feature engineering carried out in SQL by a data
scientist. The purpose is to provide a comparison point for the performance Relational Deep Learning
(RDL) models on the RelBench benchmark.

For details on the user study see section 5 and appendix C of the RelBench Paper.


## Structure
At the top level there are two noteworthy python files:
- `traing_gbdt.py`: A script that runs hyperparameter tuning for LightGBM trained on the hand-engineered
featuers for each task.
- `utils.py`: A set of utility functions useful throughout the study, most notably a function to set
up DuckDB instances of each dataset (see below).

In addition, the directories at the top level correspond to datasets in RelBench. Within each dataset
directory you will find a dataset-level exploratory data analysis (EDA) notebook, and subdirectories
for each task. In turn, each task directory contains a `feats.sql` file containing the features
engineered by a data scientist, and a notebook with dataset/model validation code.

## Setup

Install dependencies (`pip install -r requirements.txt`).

For a given dataset (eg: `rel-amazon`), set up a local DuckDB instance by running:

```shell
python -c "import utils; utils.db_setup('rel-amazon', 'amazon/amazon.db');"
```

Once you've set up a local DuckDB instance you should be able to run all the notebooks and any
additional SQL you desire.


## Training a LightGBM

Assuming you have set up the DuckDB instance as indicated above, you can train a LightGBM with the
following command:

```shell
python train_gbdt.py --dataset rel-amazon --task user-churn --generate_feats
```
