import pandas as pd

from utils import (
    remove_textures, combine_products_creation_information,
)
from data_modeling.base import BaseDataModeler
from data_processing import ProductAggregator


class ReferenceDataModeler(BaseDataModeler):
    """
    A modeler class to prepare reference data models that gives 
    manufacturing information on every product.
    """

    def __init__(self, config):
        """
        Initializes the class with paths to regular and ordered items
        Args:
            config (dict): Configuration dictionary containing file paths
        """
        super().__init__(config)
        self.ordered_items_file_path = config['ORDERED_ITEMS_MATERIAL_REQUIREMENTS_PATH']
        self.regular_items_file_path = config['REGULAR_ITEMS_MATERIAL_REQUIREMENTS_PATH']
        self.product_engineering_categories = {}
        self.product_material_requirements_df = None
        self.load_data_frame()

    def load_data_frame(self):
        """
        Prepares and aggregated manufaturing dataframes into a single DataFrame
        """
        # Load ordered items and concatenate into a single DataFrame
        try:
            all_items_on_order = pd.read_excel(
                self.ordered_items_file_path, sheet_name=None)
            # The following df includes regular products + ordered items
            # Including details of from what stock source they have been manufactured
            big_dataframe = pd.concat(
                all_items_on_order.values(), ignore_index=True)
        except Exception as e:
            print(f'Error loading ordered items: {e}')
            return
        # Use relevant columns
        big_dataframe = big_dataframe[
            ['ITEM NAME', 'QTY', 'WORK METHODE', 'MATERIALS SIZES(MM)']
        ].rename(
            columns={'MATERIALS SIZES(MM)': 'Component Sizes',
                     'WORK METHODE': 'WORK TYPE'}
        )
        big_dataframe['MOD_ITEM NAME'] = big_dataframe['ITEM NAME'].apply(
            lambda x: remove_textures(x).upper()
        )
        big_dataframe.drop(columns=['ITEM NAME'], inplace=True)
        big_dataframe.rename(
            columns={'MOD_ITEM NAME': 'ITEM NAME'}, inplace=True)
        big_dataframe.drop_duplicates(
            subset='ITEM NAME', keep='first', inplace=True)
        try:
            regular_items = pd.read_excel(self.regular_items_file_path)
        except Exception as e:
            print(f'Error loading regular items: {e}')
        self.product_material_requirements_df = ProductAggregator.combine_products_creation_information(
            regular_items, big_dataframe).fillna(0.0).drop_duplicates(subset='ITEM', keep='first')
        self.product_material_requirements_df = self.product_material_requirements_df[
            self.product_material_requirements_df['MATERIALS SIZES(MM)'] != 0
        ]

    def clean_and_prepare_data(self):
        pass
