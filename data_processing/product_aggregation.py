from typing import Optional

import pandas as pd

from utils import get_column_by_keyword


class ProductAggregator:
    @staticmethod
    def combine_products_creation_information(
         regular_items_df: pd.DataFrame, items_on_order_df: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
            """
            Merges information of two dataframes and aligns their matching columns

            Parameters
            - regular_items_df: This DataFrame contains information on the
            standard products
            - items_on_order_df: This DataFrame contains information on the unique items
            on order.
            
            Returns
            A larger DataFrame after concatenating both the DataFrames from the inputs.
            Both Input Parameters need to have information on how the products are made
            """
            item_column_reg = get_column_by_keyword(regular_items_df, 'item')
            item_column_orders = get_column_by_keyword(items_on_order_df, 'item')
            # FallBack if the item columns are not found
            if not item_column_reg or not item_column_orders:
                print('Item column not found in at least one of the DataFrames')
                return None
            rename_dictionary = {
                item_column_orders: item_column_reg,
                'Component Sizes': 'MATERIALS SIZES(MM)',
                'WORK TYPE': 'WORK METHOD'
            }
            items_on_order_df_renamed = items_on_order_df.rename(columns=rename_dictionary)
            columns_to_keep = [item_column_reg, 'WORK METHOD', 'MATERIALS SIZES(MM)']
            orders_df_relevant_columns = items_on_order_df_renamed[columns_to_keep]
            # Concatenate the DataFrames
            concatenated_df = pd.concat([regular_items_df, orders_df_relevant_columns])
            return concatenated_df
    