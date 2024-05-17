import duckdb
from jinja2 import Template
import pandas as pd
from relbench.datasets import get_dataset
from sklearn.feature_selection import mutual_info_classif


def db_setup(db_name: str, cache_dir: str | None = None):
    conn = duckdb.connect(db_name)
    dataset = get_dataset(name='rel-amazon', process=True)
    review = dataset.db.table_dict['review'].df  # noqa
    customer = dataset.db.table_dict['customer'].df  # noqa
    product = dataset.db.table_dict['product'].df  # noqa
    conn.sql('create table review as select * from review')
    conn.sql('create table customer as select * from customer')
    conn.sql('create table product as select * from product')
    for task_name in ['churn', 'ltv', 'product-ltv', 'product-churn']:
        task = dataset.get_task(f'rel-amazon-{task_name}', process=True)
        train_table = task.train_table.df  # noqa
        val_table = task.val_table.df  # noqa
        test_table = task.test_table.df  # noqa
        if task_name in ['product-ltv', 'product-churn']:
            task_name = task_name.replace('-', '_')
        conn.sql(f'create table {task_name}_train as select * from train_table')
        conn.sql(f'create table {task_name}_val as select * from val_table')
        conn.sql(f'create table {task_name}_test as select * from test_table')
    conn.close()


def render_jinja_sql(query: str, context: dict) -> str:
    return Template(query).render(context)


def feature_summary_df(df: pd.DataFrame, y_col: str) -> pd.DataFrame:
    y = df[y_col]
    df = df.drop(y_col, axis=1)
    return pd.DataFrame(
        {
            'Label Corr.': df.corrwith(y),
            'Label MI': mutual_info_classif(df.fillna(-1).values, y),
            'NaN %': df.isna().mean(),
        },
        index=df.columns
    ).sort_values(by='Label Corr.', ascending=False)
