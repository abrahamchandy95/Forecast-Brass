from typing import Dict

import pandas as pd

from data_modeling.base import BaseDataModeler
from data_processing import SupplyChainDataPrep, DescriptionDimensionProcessor
from utils import calculate_rod_top_area, calculate_volume_from_weight


class BrassStockModeler(BaseDataModeler):
    """
    Models the raw brass stock data used for manufacturing
    the main line of production
    """
    def __init__(self, live_sheets: Dict[str, pd.DataFrame]):
        super().__init__(live_sheets)
        self.supply_chain_data_prep = SupplyChainDataPrep(self.live_sheets)
        self.processor = DescriptionDimensionProcessor()
        self.raw_stock_available = None
        self.load_data_frame()
        self.clean_and_prepare_data()
        self.create_inventory_dictionary()
    
    def load_data_frame(self):
        try:
            self.raw_stock_available = self.live_sheets['RAW MATERIALS MAIN ORDERS']
        except KeyError:
            raise ValueError('Specified sheet was not found in live_sheets')

    def clean_and_prepare_data(self):
        """
        Clean the loaded data frames necessary
        """
        self.raw_stock_available = self.supply_chain_data_prep.organize_raw_stock_df(
            'RAW MATERIALS MAIN ORDERS'
        )
        self.prepare_raw_stock_dataframes()
    
    def prepare_raw_stock_dataframes(self):
        """
        This function cleans and organizes data exported 
        from 'RAW MATERIALS FOR MAIN ORDERS' sheet
        """
        df_for_rod_stock = self.raw_stock_available [
            self.raw_stock_available['Stock Type'].str.contains(
                'Rod', case=False, na=False
            )
        ]
        df_for_sheet_and_patti = self.raw_stock_available [
            self.raw_stock_available['Stock Type'].str.contains(
                'Sheet|Patti', case=False, na=False
            )
        ]
        # Rod Stock Data
        self.rod_inventory = self.processor.add_rod_dimensions_to_dataframe(
            df_for_rod_stock, 'Dimensions'
        )
        self.rod_inventory['Top Circular Area (mm^2)'] = self.rod_inventory.apply(
            lambda x: calculate_rod_top_area(
                x['Stock Type'], x['Dimensions']
            ), axis=1
        )
        self.rod_inventory['Available Volume (cm^3)'] = (
            self.rod_inventory['Current Stock (kg)'].apply(
                calculate_volume_from_weight
            )
        )
        # Patti and Sheet Stock Data
        self.sheet_patti_inventory = self.processor.add_non_rod_dimensions_to_dataframe(
            df_for_sheet_and_patti, 'Dimensions'
        )
        self.sheet_patti_inventory['Area'] = (
            self.sheet_patti_inventory['Dimension_1'] \
                * self.sheet_patti_inventory['Dimension_2']
        )
        self.sheet_patti_inventory['Available Volume (cm^3)'] = (
            self.sheet_patti_inventory['Current Stock (kg)'].apply(
                calculate_volume_from_weight
            )
        )
    
    def create_inventory_dictionary(self):
        """Prepare the inventory dictionary for easy access."""
        self.inventory_dict = {
            'Rods': self.rod_inventory,
            'Patti_Sheets': self.sheet_patti_inventory
        }
        