import argparse
import os
import numpy as np
import time

import duckdb
from relbench.datasets import get_dataset
from torch_frame import TaskType
from torch_frame.gbdt import LightGBM, XGBoost
from torch_frame.data import Dataset
from torch_frame.typing import Metric

from inferred_stypes import task_to_stypes
import utils

SEED = 42
DATASET_TO_DB = {
    'rel-stack': 'stack_exchange/stackex.db',
    'rel-amazon': 'amazon/amazon.db',
    'rel-hm': 'hm/hm.db',
    'rel-f1': 'f1/f1.db',
}
TASK_PARAMS = {
    'rel-stack-user-engagement': {
        'dir': 'stack_exchange/engage',
        'target_col': 'contribution',
        'table_prefix': 'engage',
        'identifier_cols': ['OwnerUserId', 'timestamp'],
        'tune_metric': Metric.ROCAUC,
        'task_type': TaskType.BINARY_CLASSIFICATION,
    },
    'rel-stack-user-badge': {
        'dir': 'stack_exchange/badges',
        'target_col': 'WillGetBadge',
        'table_prefix': 'badges',
        'identifier_cols': ['UserId', 'timestamp'],
        'tune_metric': Metric.ROCAUC,
        'task_type': TaskType.BINARY_CLASSIFICATION,

    },
    'rel-stack-post-votes': {
        'dir': 'stack_exchange/votes',
        'target_col': 'popularity',
        'table_prefix': 'votes',
        'identifier_cols': ['PostId', 'timestamp'],
        'tune_metric': Metric.MAE,
        'task_type': TaskType.REGRESSION,

    },
    'rel-amazon-user-churn': {
        'dir': 'amazon/churn',
        'target_col': 'churn',
        'table_prefix': 'churn',
        'identifier_cols': ['customer_id', 'timestamp'],
        'tune_metric': Metric.ROCAUC,
        'task_type': TaskType.BINARY_CLASSIFICATION,
    },
    'rel-amazon-user-ltv': {
        'dir': 'amazon/ltv',
        'target_col': 'ltv',
        'table_prefix': 'ltv',
        'identifier_cols': ['customer_id', 'timestamp'],
        'tune_metric': Metric.MAE,
        'task_type': TaskType.REGRESSION,
    },
    'rel-amazon-item-churn': {
        'dir': 'amazon/product-churn',
        'target_col': 'churn',
        'table_prefix': 'product_churn',
        'identifier_cols': ['product_id', 'timestamp'],
        'tune_metric': Metric.ROCAUC,
        'task_type': TaskType.BINARY_CLASSIFICATION,
    },
    'rel-amazon-item-ltv': {
        'dir': 'amazon/product-ltv',
        'target_col': 'ltv',
        'table_prefix': 'product_ltv',
        'identifier_cols': ['product_id', 'timestamp'],
        'tune_metric': Metric.MAE,
        'task_type': TaskType.REGRESSION,
    },
    'rel-hm-sales': {
        'dir': 'hm/sales',
        'target_col': 'sales',
        'table_prefix': 'sales',
        'identifier_cols': ['article_id', 'timestamp'],
        'tune_metric': Metric.MAE,
        'task_type': TaskType.REGRESSION,
    },
    'rel-hm-churn': {
        'dir': 'hm/churn',
        'target_col': 'churn',
        'table_prefix': 'churn',
        'identifier_cols': ['customer_id', 'timestamp'],
        'tune_metric': Metric.ROCAUC,
        'task_type': TaskType.BINARY_CLASSIFICATION,
    },
    'rel-f1-position': {
        'dir': 'f1/position',
        'target_col': 'position',
        'table_prefix': 'position',
        'identifier_cols': ['driverId', 'date'],
        'tune_metric': Metric.MAE,
        'task_type': TaskType.REGRESSION,
    },
    'rel-f1-dnf': {
        'dir': 'f1/dnf',
        'target_col': 'did_not_finish',
        'table_prefix': 'dnf',
        'identifier_cols': ['driverId', 'date'],
        'tune_metric': Metric.ROCAUC,
        'task_type': TaskType.BINARY_CLASSIFICATION,
    },
    'rel-f1-qualifying': {
        'dir': 'f1/qualifying',
        'target_col': 'qualifying',
        'table_prefix': 'qualifying',
        'identifier_cols': ['driverId', 'date'],
        'tune_metric': Metric.ROCAUC,
        'task_type': TaskType.BINARY_CLASSIFICATION,
    },
}
NUM_TRIALS = 10


def map_preds(feats_df, labels, identifier_cols, preds):
    """ Corrects shuffling that may have occurred during feature generation. """
    idx_map = {
        tuple(row.to_list()): i for i, (_, row) in enumerate(feats_df[identifier_cols].iterrows())
    }
    new_preds = np.array(
        [preds[idx_map[tuple(row.to_list())]] for _, row in labels[identifier_cols].iterrows()]
    )
    return new_preds


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument Parser')
    parser.add_argument('--dataset', '-d', type=str, help='Path to the dataset')
    parser.add_argument('--task', '-t', type=str, help='Task to perform')
    parser.add_argument('--booster', '-b', type=str, default='lgbm', help='One of "xgb" or "lgbm"')
    parser.add_argument('--subsample', '-s', type=int, default=0,
                        help=(
                            'If provided, use a subset of the training set to speed up training. '
                            'If generate_feats is set, features will only be generated for this '
                            'subset. '
                        ))
    parser.add_argument('--generate_feats', action='store_true',
                        help='Whether to generate features specified in feats.sql')
    parser.add_argument('--drop_cols', nargs='+', default=[], help='Columns to drop')
    args = parser.parse_args()
    task_params = TASK_PARAMS[args.task]
    conn = duckdb.connect(DATASET_TO_DB[args.dataset])
    if args.generate_feats:
        print('Generating features.')
        start = time.time()
        with open(os.path.join(task_params['dir'], 'feats.sql')) as f:
            template = f.read()
        # create train, val and test features
        for s in ['train', 'val', 'test']:
            print(f'Creating {s} table')
            query = utils.render_jinja_sql(template, dict(set=s, subsample=args.subsample))
            conn.sql(query)
            print(f'{s} table created')
        print(f'Features generated in {time.time() - start:,.0f} seconds.')

    train_df = conn.sql(f'select * from {task_params["table_prefix"]}_train_feats').df()
    val_df = conn.sql(f'select * from {task_params["table_prefix"]}_val_feats').df()
    test_df = conn.sql(f'select * from {task_params["table_prefix"]}_test_feats').df()
    conn.close()
    col_to_stype = task_to_stypes[args.task]
    drop_cols = task_params['identifier_cols'] + args.drop_cols
    train_df = train_df.drop(args.drop_cols, axis=1)
    val_df = val_df.drop(args.drop_cols, axis=1)
    for col in args.drop_cols:
        del col_to_stype[col]
    if args.subsample > 0 and not args.generate_feats:
        train_df = train_df.sample(args.subsample, replace=False, random_state=SEED)
    print('Materializing torch-frame dataset.')
    start = time.time()
    train_dset = Dataset(
        train_df,
        col_to_stype=col_to_stype,
        target_col=task_params['target_col'],
    ).materialize()
    val_tf = train_dset.convert_to_tensor_frame(val_df)
    print(f'Materialized torch-frame dataset in {time.time() - start:,.0f} seconds.')
    print(
        f'Train Size: {train_dset.tensor_frame.num_rows:,} x {train_dset.tensor_frame.num_cols:,}'
    )

    booster = LightGBM if args.booster == 'lgbm' else XGBoost
    if task_params['task_type'] == TaskType.BINARY_CLASSIFICATION:
        gbdt = booster(task_params['task_type'], num_classes=2, metric=task_params['tune_metric'])
    elif task_params['task_type'] == TaskType.REGRESSION:
        gbdt = booster(task_params['task_type'], metric=task_params['tune_metric'])
    print('Starting hparam tuning.')
    start = time.time()
    gbdt.tune(tf_train=train_dset.tensor_frame, tf_val=val_tf, num_trials=NUM_TRIALS)
    print(f'Hparam tuning completed in {time.time() - start:,.0f} seconds.')
    model_path = os.path.join(task_params['dir'], f'{args.task}_{args.booster}.json')
    print(f'Saving model to "{model_path}".')
    gbdt.save(model_path)
    print()

    print('Evaluating model.')
    dset = get_dataset(name=args.dataset, process=True)
    task = dset.get_task(args.task, process=True)
    print()
    pred = gbdt.predict(tf_test=val_tf).numpy()
    assert len(task.val_table.df) == len(val_df), 'Val: feature df does not match label df!'
    pred = map_preds(val_df, task.val_table.df, task_params['identifier_cols'], pred)
    print(f"Val: {task.evaluate(pred, task.val_table)}")
    print()
    test_tf = train_dset.convert_to_tensor_frame(test_df)
    assert len(task.test_table.df) == len(test_df), 'Test: feature df does not match label df!'
    pred = gbdt.predict(tf_test=test_tf).numpy()
    pred = map_preds(test_df, task.test_table.df, task_params['identifier_cols'], pred)
    print(f"Test: {task.evaluate(pred)}")
