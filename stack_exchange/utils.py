import pandas as pd
from sklearn.feature_selection import mutual_info_classif


def feature_summary_df(val_df: pd.DataFrame, y_col: str) -> pd.DataFrame:
    y = val_df[y_col]
    val_df = val_df.drop(y_col, axis=1)
    return pd.DataFrame(
        {
            'Label Corr.': val_df.corrwith(y),
            'Label MI': mutual_info_classif(val_df.fillna(-1).values, y),
            'NaN %': val_df.isna().mean(),
        },
        index=val_df.columns
    ).sort_values(by='Label Corr.', ascending=False)
