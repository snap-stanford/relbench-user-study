import duckdb
from jinja2 import Template
import pandas as pd
from relbench.datasets import get_dataset
from sklearn.feature_selection import mutual_info_classif


# Run `python -c "from utils import db_setup; db_setup('stackex.db');` to create the database
def db_setup(db_name: str, cache_dir: str | None = None):
    conn = duckdb.connect(db_name)
    dataset = get_dataset(name='rel-stackex', process=True, cache_dir=cache_dir)
    users = dataset.db.table_dict['users'].df  # noqa
    posts = dataset.db.table_dict['posts'].df  # noqa
    votes = dataset.db.table_dict['votes'].df  # noqa
    badges = dataset.db.table_dict['badges'].df  # noqa
    comments = dataset.db.table_dict['comments'].df  # noqa
    post_history = dataset.db.table_dict['postHistory'].df  # noqa
    conn.sql('create table users as select * from users')
    conn.sql('create table posts as select * from posts')
    conn.sql('create table votes as select * from votes')
    conn.sql('create table badges as select * from badges')
    conn.sql('create table comments as select * from comments')
    conn.sql('create table post_history as select * from post_history')
    for task_name in ['engage', 'badges', 'votes']:
        task = dataset.get_task(f'rel-stackex-{task_name}', process=True)
        train_table = task.train_table.df  # noqa
        val_table = task.val_table.df  # noqa
        test_table = task.test_table.df  # noqa
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
