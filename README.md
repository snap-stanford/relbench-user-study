# Relbench User Study

## Description

This project aims to conduct a user study for Relbench, a benchmarking tool for relational databases.
The goal of the study is to benchmark the performance of a classical ML model (LightGBM) with feature
engineering conducted in SQL by a data scientist.

## Study Design

TODO Write

## Structure

The top level directories correspond to datasets in Relbench. Within each dataset you will find a
dataset-level exploratory data analysis (EDA) notebook, a utils module (which among other things can
help you setup a DuckDB instance of the dataset) and subdirectories for each task. In turn, each
task directory contains a `feats.sql` file containing the features engineered by a data scientist,
and a notebook which uses those features to train a LightGBM model and evaluate its performance.

## Setup

Install dependencies (TODO add reqs.txt or pyproject.toml). Then setup a local DuckDB instance of
the dataset by runnig (eg):

```shell
python -c "import utils; utils.db_setup("rel-amazon", "amazon/amazon.db");"
```

Once you've set up a local DuckDB instance you should be able to run all the notebooks and any
additional SQL you desire.
