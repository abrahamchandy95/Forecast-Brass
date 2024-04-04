from typing import Dict, TYPE_CHECKING

import numpy as np
import pandas as pd

from utils import get_column_by_keyword, compute_volume
if TYPE_CHECKING:
    from .product_area_calculator import ProductAreaCalculator


class ProductVolumeCalculator:
    def __init__(self, area_calculator: 'ProductAreaCalculator'):
        # Instance of area_calculator class
        self.area_calculator = area_calculator

    @staticmethod
    def adjust_volume_for_posts(
        dataframe: pd.DataFrame, volume_column: str
    ) -> pd.DataFrame:
        df = dataframe.copy()
        item_column = get_column_by_keyword(df, 'item')
        df[volume_column] = np.where(
            df[item_column].str.contains('DP|AP|BP'),
            df[volume_column] * 2, df[volume_column]
        )
        return df

    @staticmethod
    def adjust_volume_for_btb_products(dataframe: pd.DataFrame, item_column: str):
        """
        Doubles the volume for any back-to-back product
        """
        for col in dataframe.columns:
            if col.endswith('Volume'):
                dataframe[col] = dataframe.apply(
                    lambda x: x[col] * 2 if 'BTB' in x[item_column] else x[col], axis=1
                )
        return dataframe
    
    def initialize_volume_columns(
        self, df: pd.DataFrame, volume_columns: list
    ) -> pd.DataFrame:
        for col in volume_columns:
            if col not in df.columns:
                df[col] = np.nan
        return df
        

    def calculate_cylinder_volume(
        self, dataframes_dictionary: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:

        result_dict = {
            key: df.copy() for key, df in dataframes_dictionary.items()
        }
        relevant_keys = [
            'round_rod', 'round_rect_single_rods_stock', 'pipe_composite_stock',
            'three_distinct_round_rods', 'two_distinct_round_rods',
            'round_square_single_rods_stock', 'ring_pull_stock'
        ]
        for key, dataframe in result_dict.items():
            if key not in relevant_keys:
                continue

            cylinder_counts = sum(
                1 for col in dataframe.columns if col.startswith('Diameter')
            )
            volume_columns = [f'Cylinder_{i}_Volume' for i in range(1, cylinder_counts + 1)]
            dataframe = self.initialize_volume_columns(dataframe, volume_columns)
            
            item_column = get_column_by_keyword(dataframe, 'item')
            # the height for products is not consistent in hardcoded data
            for i in range(1, cylinder_counts + 1):
                area_column = f'Circular_Area_{i}_Matched'
                volume_column = f'Cylinder_{i}_Volume'
                # Inline get_height_column logic
                if key == 'ring_pull_stock':
                    height_column = 'Diameter_1'
                elif key in ['round_rod', 'round_square_single_rods_stock', 'round_rect_single_rods_stock'] or \
                        (key in ['pipe_composite_stock', 'three_distinct_round_rods'] and i == 3):
                    height_column = 'Length Dim (mm)'
                elif key == 'two_distinct_round_rods':
                    height_column = 'Top Dim (mm)' if i == 2 else 'Length Dim (mm)'
                else:
                    height_column = 'Top Dim (mm)'

                dataframe = compute_volume(
                    dataframe, area_column, height_column, volume_column
                )
            if key in ['round_rect_single_rods_stock', 'three_distinct_round_rods'] and \
                    'Cylinder_1_Volume' in dataframe.columns:
                dataframe = self.adjust_volume_for_posts(
                    dataframe, 'Cylinder_1_Volume')
            if key == 'two_distinct_round_rods' and 'Cylinder_2_Volume' in dataframe.columns:
                dataframe = self.adjust_volume_for_posts(
                    dataframe, 'Cylinder_2_Volume')

            # Apply 'BTB' check in a single step for all volume columns
            dataframe = self.adjust_volume_for_btb_products(
                dataframe, item_column)
            result_dict[key] = dataframe

        return result_dict

    def calculate_cuboid_volume(
        self, dataframes_dictionary: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        # Keys for calculating the volume of the first cuboid
        top_dim_keys = {
            'ring_pull_stock', 'round_rect_single_rods_stock',
            'round_square_single_rods_stock', 'two_rectangular_plates'
        }
        length_dim_keys = {'plate', 'square_rod'}
        # Height of the second cuboid per product in the following keys
        second_cuboid_heights = {
            'two_rectangualar_plates': 'Length Dim (mm)',
            'ring_pull_stock': 'Length Dim (mm)'
        }
        applicable_keys = top_dim_keys.union(length_dim_keys)
        resulting_dict = {key: df.copy() for key, df in dataframes_dictionary.items()}
        for key, df_copy in resulting_dict.items():
            if key not in applicable_keys:
                continue
            volume_columns = [
                    'Cuboid_1_Volume', 'Cuboid_2_Volume', 'Squared_1_Volume'
            ]
            df_copy = self.initialize_volume_columns(df_copy, volume_columns)
            item_column = get_column_by_keyword(df_copy, 'item')
            # First Cuboid
            height_column_1 = 'Top Dim (mm)' if key in top_dim_keys else 'Length Dim (mm)'
            area_col_1 = (
                'Square_Area_1' if key in ['square_rod', 'round_square_single_rods_stock']
                else 'Rectangular_Area_1'
            )
            volume_col_1 = 'Squared_1_Volume' if 'Square' in area_col_1 else 'Cuboid_1_Volume'
            df_copy = compute_volume(
                df_copy, area_col_1, height_column_1, volume_col_1)

            # Second cuboid for the same product
            if key in second_cuboid_heights:
                compute_volume(df_copy, 'Rectangular_Area_2',
                               second_cuboid_heights[key], 'Cuboid_2_Volume')
                if key == 'two_rectangular_plates' and 'DP173' not in df_copy[item_column].astype(str).values:
                    df_copy = self.adjust_volume_for_posts(
                        df_copy, 'Cuboid_2_Volume')
            df_copy = self.adjust_volume_for_btb_products(df_copy, item_column)
            resulting_dict[key] = df_copy
            resulting_dict[key] = df_copy.dropna(axis=1, how='all')
        return resulting_dict

    def calculate_sheet_volume(self, dataframe):
        df = dataframe.copy()
        item_column = get_column_by_keyword(df, 'item')
        areas = df.apply(
            lambda row: self.area_calculator.parse_areas_for_flush_pulls(
                row['Component Sizes'], row['Length Dim (mm)']
            ), axis=1
        )
        df['Top_Area'], df['Side_Area'] = zip(*areas)
        df['Front_Plate_Area'] = df.apply(
            self.area_calculator.calculate_front_plate_area, axis=1
        )
        df['Back_Plate_Area'] = df.apply(
            self.area_calculator.calculate_back_plate_area, axis=1)
        thickness_multiplier = df['Component Sizes'].apply(
            lambda x: 1.6 if '1.6' in x else 3)
        df['Sheet_Volume'] = (
            (df['Top_Area'] + df['Side_Area']
                + df['Front_Plate_Area'] + df['Back_Plate_Area']
             ) * thickness_multiplier
        )
        if item_column:
            df['Sheet_Volume'] = df.apply(
                lambda x: x['Sheet_Volume'] * 2 if 'TE' in x[item_column]
                else x['Sheet_Volume'], axis=1
            )

        df['Matched_FirstCol'] = 'Brass Sheet'
        df['Matched_SecondCol'] = df.apply(
            lambda row: 'BRASS SHEET 1.6MM (48"X14"X1.6MM)' if '1.6' in row['Component Sizes']
            else 'BRASS SHEET 3MM', axis=1
        )
        return df
