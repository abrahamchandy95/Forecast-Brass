from typing import List, Dict

import pandas as pd

from data_processing.constants import RAW_BRASS_STOCK


class SupplyChainDataPrep:
    def __init__(self, live_sheets: Dict[str, pd.DataFrame]):
        """
        Initializes the SupplyChainDataPrep with loaded sheets data
        Parameters:
        - live_sheets (Dict[str, pd.DataFrame]): A dictionary of DataFrames
          loaded from live Google Sheets, keyed by sheet name
        """
        self.live_sheets = live_sheets

    def clean_orders_sheet(
        self, sheet_name: str, condition_columns: List[str],
        relevant_columns: List[str], rename_columns: Dict[str, str] = None
    ) -> pd.DataFrame:
        """
        Prepares the relevant sheets that track orders
        Auto-Filters for necessary data.

        Parameters:
        - sheet_name (str): The name of the sheet that needs cleaning
        - condition_columns (List[(str)]): columns used to filter out obsolete data
        - relevant_columns (List[(str)]): columns needed for the project

        Returns:
        pd.DataFrame: Cleaned sheet of all orders with only necessary columns
        """
        if sheet_name not in self.live_sheets:
            raise ValueError(
                f'Sheet name \'{sheet_name}\' not in provided data.')
        df = self.live_sheets[sheet_name].copy()
        condition = pd.Series([True] * len(df), index=df.index)

        for col in condition_columns:
            if col in df.columns:
                condition &= (df[col].isna() | ('' == df[col]))
        orders = df[condition].copy()
        # Convert values in numeric columns to int64
        for column in ['P.O', 'QTY']:
            orders.loc[:, column] = pd.to_numeric(
                orders[column], errors='coerce')
            orders.dropna(subset=[column], inplace=True)
            orders.loc[:, column] = orders[column].astype('Int64')
        # Final DataFrame only contains relevant columns
        orders = orders.loc[:, relevant_columns]
        if rename_columns:
            orders.rename(columns=rename_columns, inplace=True)
        return orders

    def organize_raw_stock_df(self, sheet_name: str) -> pd.DataFrame:
        """
        Function that organizes the sheet containing stock of raw material

        Parameters:
        -df (Dataframe): The raw stock dataframe that needs to be organized

        Returns:
        - DataFrame: The organized DataFrame with updated column names and features
        """
        if sheet_name not in self.live_sheets:
            raise ValueError(
                f'Sheet name \'{sheet_name}\' not in provided data.')
        df = self.live_sheets[sheet_name].copy()
        new_header = df.iloc[0, 1:]
        df = df.iloc[1:, 1:].copy()
        df.columns = new_header
        df.reset_index(drop=True, inplace=True)
        # Replace '-' with 0 in 'Minimum Stock in kgs' column
        df['Minimum Stock in KGs'] = df['Minimum Stock in KGs'].replace(
            '-', '0').astype(str)
        # Convert to numeric
        columns_to_convert = ['Closing Wt.', 'Minimum Stock in KGs']
        for col in columns_to_convert:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=columns_to_convert)
        # Add column for material type
        df['Material Type'] = None
        for index, row in df.iterrows():
            first_col = str(row[df.columns[0]]).lower()
            matched_stock = next(
                (
                    stock_name for stock_name in RAW_BRASS_STOCK
                    if stock_name.lower() in first_col
                ),
                None
            )
            if matched_stock:
                df.at[index, 'Material Type'] = matched_stock
        # Correctly name columns
        rename_dict = {
            'ROUND ROD': 'Dimensions',
            'Closing Wt.': 'Current Stock (kg)',
            'Minimum Stock in KGs': 'Minimum Stock (kg)',
            'Material Type': 'Stock Type'
        }
        df_renamed = df.rename(columns=rename_dict)
        column_order = [
            'Stock Type', 'Dimensions',
            'Current Stock (kg)', 'Minimum Stock (kg)'
        ]
        df_reordered = df_renamed[column_order]
        return df_reordered
