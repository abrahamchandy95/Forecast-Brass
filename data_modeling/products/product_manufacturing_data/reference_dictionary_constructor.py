from typing import Dict, Tuple, Callable
import pandas as pd

from data_modeling.base import BaseDataModeler
from data_modeling.products.open_orders import ReferenceDataModeler
from utils import get_column_by_keyword


class ReferenceDictionaryConstructor(BaseDataModeler):
    """
    Constructs a detailed product dictionary
    categorizing products by stock requirements
    """

    def __init__(self, config):
        super().__init__(config)
        self.reference_data_modeler = ReferenceDataModeler(config)
        self.product_material_requirements_df = \
            self.reference_data_modeler.product_material_requirements_df
        self.product_engineering_categories = {}
        self.categorize_product_per_stock_requirement()
        self.finalize_dictionary_construction()

    def clean_and_prepare_data(self):
        pass

    def load_data_frame(self):
        pass

    def categorize_product_per_stock_requirement(self):
        """
        Categorize products by stock requirements based on component sizes.
        """
        df = self.product_material_requirements_df
        size_column = get_column_by_keyword(df, 'size')
        if size_column is None:
            raise ValueError("No suitable size column found in DataFrame")
        composite_mask = df[size_column].str.contains(
            '&', case=False, na=False)
        composite_materials_df = df[composite_mask]
        homogeneous_materials_df = df[~composite_mask]
        # Categorize the remaining data
        self.product_engineering_categories = {
            'composite_materials': composite_materials_df,
            'scrap': homogeneous_materials_df[
                homogeneous_materials_df[size_column].str.contains(
                    'scrap', case=False, na=False)
            ],
            'round_rod': homogeneous_materials_df[
                homogeneous_materials_df[size_column].str.contains(
                    'dia', case=False, na=False)
            ],
            'plate': homogeneous_materials_df[
                homogeneous_materials_df[size_column].str.contains(
                    'X', case=False, na=False)
            ],
            'square_rod': homogeneous_materials_df[
                homogeneous_materials_df[size_column].str.contains(
                    'sq', case=False, na=False)
            ],
        }
        for category, dataframe in self.product_engineering_categories.items():
            self.product_engineering_categories[category] = dataframe.rename(
                columns={size_column: 'Component Sizes'},
            )

    def segregate_composite_components(
            self, df: pd.DataFrame, column_name='Component Sizes'
    ) -> Dict[str, pd.DataFrame]:
        """
        Segregates components within a DataFrame into predefined categories
        based on certain conditions.

        Args:
            df (pd.DataFrame): The DataFrame to process.
            column_name (str, optional): The name of the column to apply conditions to. 

        Returns:
            dict: A dictionary of DataFrames categorized by the specified conditions.
        """
        df_reset = df.reset_index(drop=True)
        composite_dfs = {
            'ring_pull_stock': None, 'round_rect_single_rods_stock': None,
            'pipe_composite_stock': None, 'three_distinct_round_rods': None,
            'two_distinct_round_rods': None, 'round_square_single_rods_stock': None,
            'two_rectangular_plates': None, 'metal_sheet': None,
        }
        used_mask = pd.Series([False]*len(df_reset))
        conditions = self.get_split_conditions()
        for key, condition in conditions.items():
            composite_dfs[key], used_mask = self.apply_conditions_to_split_composite_df(
                df_reset, condition, used_mask, column_name
            )
        return composite_dfs

    def get_split_conditions(self) -> Dict[str, Callable[[pd.Series], pd.Series]]:
        """
        Defines the conditions used to further segregate 
        composite components into categories.

        Returns:
            dict: A dictionary where keys are category names and values are 
            that define the conditions for each category.
        """
        return {
            'ring_pull_stock': lambda x: (
                (2 == x.str.count(r'(?i)x')) & (1 == x.str.count('Dia'))
            ),
            'round_rect_single_rods_stock': lambda x: (
                (1 == x.str.count('X')) & (1 == x.str.count('Dia'))
            ),
            'pipe_composite_stock': lambda x: (
                1 == x.str.count('Pipe')
            ),
            'three_distinct_round_rods': lambda x: (
                3 == x.str.count('Dia')
            ),
            'two_distinct_round_rods': lambda x: (
                (2 == x.str.count('Dia')) & (~x.str.contains('Pipe'))
            ),
            'round_square_single_rods_stock': lambda x: (
                (1 == x.str.count('Dia')) & (1 == x.str.count('Sq'))
            ),
            'two_rectangular_plates': lambda x: (
                2 == x.str.count('Rect')
            ),
            'metal_sheet': lambda x: (
                x.str.contains('Sheet')
            ),
        }

    def apply_conditions_to_split_composite_df(
            self, df: pd.DataFrame, condition: Callable[[pd.Series], pd.Series],
            used_mask: pd.Series, column_name: str
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Applies conditions to a DataFrame to filter components into categories.

        Args:
            df (pd.DataFrame): The DataFrame to process.
            condition (function): The condition function to apply to the DataFrame.
            used_mask (pd.Series): A Series indicating rows already categorized.
            column_name (str): The name of the column to apply conditions to.

        Returns:
            tuple: A filtered DataFrame that meets the condition, and an updated mask.
        """
        current_mask = condition(df[column_name]) & ~used_mask
        df_filtered_by_stock_type = df[current_mask].copy()
        updated_mask = used_mask | current_mask
        return df_filtered_by_stock_type, updated_mask

    def finalize_dictionary_construction(self):
        if 'composite_materials' in self.product_engineering_categories:
            composite_df = self.product_engineering_categories['composite_materials']
            composite_data = self.segregate_composite_components(composite_df)
            self.product_engineering_categories.update(composite_data)
            self.product_engineering_categories.pop('composite_materials')
