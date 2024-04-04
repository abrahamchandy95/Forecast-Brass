import pandas as pd

from data_modeling.base import BaseDataModeler


class OrdersDataModeler(BaseDataModeler):
    def __init__(self, live_sheets):
        super().__init__(live_sheets)
        self.orders_dataframe = None
        self.load_data_frame()

    def load_data_frame(self):
        """
        Load and combine SOL and SEA orders into a single dataframe.
        """
        sol_sheet_title = 'SOL NEW CONSOLIDATED'
        sol_condition_columns = ['STATUS', 'TRACKING']
        sol_relevant_columns = [
            'P.O', 'ITEM CODE', 'FINISH', 'QTY',
            'UNIT', 'P.O DATE'
        ]
        sol_open_orders = self.supply_chain_data_prep.clean_orders_sheet(
            sol_sheet_title, sol_condition_columns, sol_relevant_columns,
            rename_columns={'ITEM CODE': 'ITEM'}
        )
        sea_sheet_title = 'SEA ORDERS'
        sea_condition_columns = ['STATUS', 'TRACKING']
        sea_relevant_columns = [
            'P.O', 'ITEM NAME', 'FINISH', 'QTY',
            'UNIT', 'P.O DATE'
        ]
        sea_open_orders = self.supply_chain_data_prep.clean_orders_sheet(
            sea_sheet_title, sea_condition_columns, sea_relevant_columns,
            rename_columns={'ITEM NAME': 'ITEM'}
        )
        
        self.orders_dataframe = pd.concat(
            [sol_open_orders, sea_open_orders],
            ignore_index=True
        )
    def clean_and_prepare_data(self):
        pass
