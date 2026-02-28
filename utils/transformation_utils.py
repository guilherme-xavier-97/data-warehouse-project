import pandas as pd

class TransformationUtils:
    @staticmethod
    def remove_nulls(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        return df.dropna(subset=[col_name])

    @staticmethod
    def remove_whitespaces(df: pd.DataFrame) -> pd.DataFrame:
        text_cols = df.select_dtypes(include=['string']).columns
        for col in text_cols:
            df[col] = df[col].str.strip()
        return df

    @staticmethod
    def remove_duplicates(df: pd.DataFrame, pk_col: str, date_col: str) -> pd.DataFrame:
        df = df.sort_values(by=[pk_col, date_col], ascending=[True, False])
        df = df.drop_duplicates(subset=pk_col, keep='first')
        return df