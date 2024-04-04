from typing import Dict
import json

import pandas as pd
import numpy as np

from data_modeling.products.product_manufacturing_data.reference_dictionary_constructor import ReferenceDictionaryConstructor
from utils import get_column_by_keyword


class DimensionUpdater:
    def __init__(self, config):
        self.reference_constructor = ReferenceDictionaryConstructor(config)
        self.product_engineering_categories = \
            self.reference_constructor.product_engineering_categories
        self.hardcoded_data_filepath = config['HARDCODED_DATA_FILEPATH']

    def update_dimensions_with_hardcoded_data(self):
        """
        Updates the dimensions for each item category based on loaded data
        """
        with open(self.hardcoded_data_filepath, 'r') as json_file:
            loaded_data = json.load(json_file)

        for category in loaded_data:
            if category in self.product_engineering_categories:
                self.product_engineering_categories[category] = self.update_product_dimensions(
                    self.product_engineering_categories[category], loaded_data[category]
                )

    def update_product_dimensions(
        self, df: pd.DataFrame, new_data: Dict
    ) -> pd.DataFrame:
        df_copy = df.copy()
        item_column = get_column_by_keyword(df_copy, 'item')
        new_dataframe = pd.DataFrame(
            new_data, columns=[item_column, 'Top Dim (mm)', 'Length Dim (mm)'])
        combined_data = df_copy.merge(
            new_dataframe, on=item_column, how='left', suffixes=('', '_new'))
        # newly uploaded values are prefered
        df_copy['Top Dim (mm)'] = np.where(
            combined_data['Top Dim (mm)_new'].notna(),
            combined_data['Top Dim (mm)_new'], df_copy['Top Dim (mm)']
        )
        df_copy['Length Dim (mm)'] = np.where(
            combined_data['Length Dim (mm)_new'].notna(),
            combined_data['Length Dim (mm)_new'], df_copy['Length Dim (mm)']
        )
        return df_copy
        