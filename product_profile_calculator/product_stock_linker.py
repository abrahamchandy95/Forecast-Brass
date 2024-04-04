from typing import Dict, Any

import numpy as np
import pandas as pd

from utils import get_column_by_keyword


class ProductSourceLinker:
    def __init__(self):
        pass

    def link_shape_to_source(
        self, dataframe_to_update: pd.DataFrame,
        stock_dictionary: Dict[str, pd.DataFrame], area_type: str
    ) -> pd.DataFrame:
        """ 
        Links products to available stock based on shape and area
        """
        if area_type in ['Circular', 'Square']:
            stock_dataframe = stock_dictionary['Rods'].copy()
        else:
            stock_dataframe = stock_dictionary['Patti_Sheets'].copy()

        df_copy = dataframe_to_update.copy()
        area_column_from_stock_data = get_column_by_keyword(
            stock_dataframe, 'area')

        area_columns = [col for col in df_copy.columns if 'area' in col.lower()
                        and area_type.lower() in col.lower() and not col.endswith('Match')]

        for area_column in area_columns:
            df_copy[area_column] = pd.to_numeric(
                df_copy[area_column], errors='coerce')
            # To prevent a repreating number of columns created
            match_col_name = f'{area_column}_Matched' if not area_column.endswith('Matched')\
                else area_column
            # Creating reference columns
            first_lookup_column = f'{match_col_name}_FirstCol'
            second_lookup_column = f'{match_col_name}_SecondCol'
            df_copy[match_col_name] = np.nan
            df_copy[first_lookup_column] = None
            df_copy[second_lookup_column] = None

            for index, row in df_copy.iterrows():
                area_value = row[area_column]
                closest_area, closest_index = None, None
                # Iterate though stock_dataframe to find matches
                for idx, stock_row in stock_dataframe.iterrows():
                    stock_area = stock_row[area_column_from_stock_data]
                    if pd.notnull(stock_area) and pd.notnull(area_value) and \
                            stock_area >= area_value:
                        if closest_area is None or (stock_area < closest_area):
                            closest_area, closest_index = stock_area, idx
                if closest_index is not None:
                    df_copy.at[index, match_col_name] = closest_area
                    df_copy.at[index, first_lookup_column] = stock_dataframe.at[
                        closest_index, stock_dataframe.columns[0]
                    ]
                    df_copy.at[index, second_lookup_column] = stock_dataframe.at[
                        closest_index, stock_dataframe.columns[1]
                    ]

        return df_copy

    def lookup_raw_stock(
        self, product_dict: Dict[str, pd.DataFrame], stock_dict: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """Links products to the source of raw stock they come from

        Args:
            product_dict: Dictionary of product DataFrames to update.
            stock_dict: Dictionary of stock DataFrames for linking.

        Returns:
            Updated product dictionary with stock information.
        """
        area_types = ['Circular', 'Rectangular', 'Square']
        updated_products_dictionary = {}
        for key, df in product_dict.items():
            if key == 'metal_sheet':
                updated_products_dictionary[key] = df
                continue
            df_updated = df.copy()
            for area_type in area_types:
                df_updated = self.link_shape_to_source(
                    df_updated, stock_dict, area_type)
            df_updated_nona = df_updated.fillna(0)
            # Convert columns ending with FirstCol and SecondCol to object after fillna
            lookup_columns = [
                col for col in df_updated_nona.columns if col.endswith(
                ('FirstCol', 'SecondCol')
                )
            ]
            for col in lookup_columns:
                df_updated_nona[col] = df_updated_nona[col].astype('object')
            updated_products_dictionary[key] = df_updated_nona
        return updated_products_dictionary
