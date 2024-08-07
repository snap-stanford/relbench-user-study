import duckdb
from jinja2 import Template
import pandas as pd
from relbench.datasets import get_dataset
from relbench.tasks import get_task
from sklearn.feature_selection import mutual_info_classif, mutual_info_regression

DATASET_INFO = {
    'rel-stack': {
        'tables': ['users', 'posts', 'votes', 'badges', 'comments', 'postHistory'],
        'tasks': ['user-engagement', 'user-badge', 'post-votes']
    },

    'rel-amazon': {
        'tables': ['review', 'customer', 'product'],
        'tasks': ['user-churn', 'user-ltv', 'product-ltv', 'product-churn']
    },

    'rel-hm': {
        'tables': ['article', 'customer', 'transactions'],
        'tasks': ['user-churn', 'item-sales'],
    },

    'rel-f1': {
        'tables': ['races', 'circuits', 'drivers', 'results', 'standings', 'constructors',
                   'constructor_results', 'constructor_standings', 'qualifying'],
        'tasks': ['driver-position', 'driver-dnf', 'driver-top3']
    },

    'rel-trial': {
        'tables': ['studies', 'outcomes', 'outcome_analyses', 'drop_withdrawals',
                   'reported_event_totals', 'designs', 'eligibilities', 'interventions',
                   'conditions', 'facilities', 'sponsors', 'interventions_studies',
                   'conditions_studies', 'facilities_studies', 'sponsors_studies'],
        'tasks': ['study-outcome', 'study-adverse', 'site-success']
    },

    'rel-event': {
        'tables': ['users', 'events', 'event_attendees', 'event_interest', 'user_friends'],
        'tasks': ['user-repeat', 'user-ignore', 'user-attendance']
    },

    'rel-avito': {
        'tables': ['AdsInfo', 'Category', 'Location', 'PhoneRequestsStream', 'SearchInfo',
                   'SearchStream', 'UserInfo', 'VisitStream'],
        'tasks': ['ad-ctr', 'user-clicks', 'user-visits']
    },
}


def db_setup(dataset_name: str, db_filename: str):
    """ Sets up a DuckDB database (at db_filename) with the tables from the specified dataset.

    Args:
        dataset_name (str): The name of the relbench dataset.
        db_filename (str): Path to the DuckDB database file.
    """
    conn = duckdb.connect(db_filename)
    dataset = get_dataset(name=dataset_name, download=True)  # noqa
    tasks = DATASET_INFO[dataset_name]['tasks']
    tables = DATASET_INFO[dataset_name]['tables']
    for table_name in tables:
        exec(f'{table_name} = dataset.get_db().table_dict["{table_name}"].df')
        conn.sql(f'create table {table_name} as select * from {table_name}')
    for task_name in tasks:
        task = get_task(dataset_name, task_name, download=True)
        train_table = task.get_table("train").df  # noqa
        val_table = task.get_table("val").df  # noqa
        test_table = task.get_table("test").df  # noqa
        task_name = task_name.replace('-', '_')
        conn.sql(f'create table {task_name}_train as select * from train_table')
        conn.sql(f'create table {task_name}_val as select * from val_table')
        conn.sql(f'create table {task_name}_test as select * from test_table')
    conn.close()


def render_jinja_sql(query: str, context: dict) -> str:
    return Template(query).render(context)


def validate_feature_tables(
    task: str, conn: duckdb.DuckDBPyConnection = None, db_filename: str = None
):
    task = task.replace('-', '_')
    if conn is None:
        conn = duckdb.connect(db_filename)
    error_count = 0
    for s in ['train', 'val', 'test']:
        table_name = f'{task}_{s}'
        print(f'Validating {s}')
        labels = conn.sql(f'select * from {table_name}').df()
        feats = conn.sql(f'select * from {table_name}_feats').df()
        print(f'{s} labels size: {len(labels):,} x {len(labels.columns):,}')
        print(f'{s} feats size: {len(feats):,} x {len(feats.columns):,}')
        # validate feats \subset labels
        joined = labels.merge(feats, how='inner', on=labels.columns.tolist(), suffixes=('', '_r'))
        if (diff := len(labels) - len(joined)) != 0:
            print(f'⚠️ {diff:,} samples are missing from feats table!')
            error_count += 1
        print()
    if error_count == 0:
        print('✅ All tables are valid!')
    else:
        print(f'❌ {error_count} errors found!')
    if db_filename is not None:
        conn.close()


def feature_summary_df(df: pd.DataFrame, y_col: str, classification: bool = True):
    y = df[y_col]
    df = df.drop(y_col, axis=1)
    invalid_cols = df.select_dtypes(exclude=['number', 'category']).columns
    df = df.drop(invalid_cols, axis=1)
    if classification:
        mi = mutual_info_classif(df.fillna(-1).values, y)
    else:
        mi = mutual_info_regression(df.fillna(-1).values, y)
    res = pd.DataFrame(
        {
            'Label Corr.': df.corrwith(y),
            'Label MI': mi,
            'NaN %': df.isna().mean(),
        },
        index=df.columns
    )
    return (
        res
        .sort_values(by='Label MI', ascending=False)
        .style
        .format({'Label Corr.': '{:.3f}', 'Label MI': '{:.3f}', 'NaN %': '{:.1%}'})
    )
