from typing import Dict, Tuple, Any

import numpy as np
import pandas as pd
import re

from utils import get_column_by_keyword


class ProductAreaCalculator:
    def __init__(self) -> None:
        pass

    def parse_areas_for_flush_pulls(
        self, component_size_str: str, length_dim: float
    ) -> Tuple[float, float]:
        pattern = re.compile(r'(\d+)\s*X\s*(\d+)', re.IGNORECASE)
        match = pattern.search(component_size_str)
        if match:
            width, height = int(match.group(1)), int(match.group(2))
            top_area = width * height * 2  # adjusted for top and bottom areas
            side_area = width * length_dim * 2  # two sides
            return top_area, side_area
        else:
            return 0, 0

    def calculate_front_plate_area(self, row: pd.Series) -> float:
        return (row['Top Dim (mm)'] + 3) * (row['Length Dim (mm)'] + 3)

    def calculate_back_plate_area(self, row: pd.Series) -> float:
        return row['Top Dim (mm)'] * row['Length Dim (mm)']

    def parse_circular_areas_into_dict(
        self, dataframes_dict: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """
        Extracts diameters from a column and adds the corresponding area
        """
        dict_with_circular_areas = {}
        for key, df in dataframes_dict.items():
            df_copy = df.copy()
            size_column = get_column_by_keyword(df_copy, 'size')
            pattern = re.compile(r'(\d+(?:\.\d+)?)\s*Dia')
            diameters_and_areas_df = pd.DataFrame(index=df_copy.index)

            for index, row in df_copy.iterrows():
                diameters = pattern.findall(row[size_column])
                for i, diameter_str in enumerate(diameters):
                    diameter = float(diameter_str)
                    radius = diameter / 2
                    area = np.pi * (radius ** 2)
                    diameter_column = f'Diameter_{i+1}'
                    area_column = f'Circular_Area_{i+1}'
                    diameters_and_areas_df.loc[index,
                                               diameter_column] = diameter
                    diameters_and_areas_df.loc[index, area_column] = area
            result_df = pd.concat(
                [df_copy, diameters_and_areas_df], axis=1, sort=False)
            dict_with_circular_areas[key] = result_df
        return dict_with_circular_areas

    def parse_plate_areas(self, string: str) -> Any:
        matches = re.findall(r'(\d+(\.\d+)?)\s*[xX]\s*(\d+(\.\d+)?)', string)
        areas = []
        for match in matches:
            length, width = float(match[0]), float(match[2])
            areas.append((length) * (width))
        return areas

    def calculate_square_area(self, string: str) -> Any:
        match = re.search(r'(\d+(\.\d+)?)\s*Sq', string, re.IGNORECASE)
        return float(match.group(1))**2 if match else None

    def calculate_areas_for_rectangular_shapes(
        self, dataframes_dict: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        updated_dataframes_dict = {key: dataframe.copy()
                                   for key, dataframe in dataframes_dict.items()}

        exclusively_rectangular_shapes = [
            'plate', 'round_rect_single_rods_stock', 'two_rectangular_plates', 'ring_pull_stock'
        ]
        for key, df in updated_dataframes_dict.items():
            if key in exclusively_rectangular_shapes:
                size_column = get_column_by_keyword(df, 'size')
                # Calculate these areas and add them to the df
                area_results = df[size_column].apply(self.parse_plate_areas)
                df['Rectangular_Area_1'] = area_results.apply(
                    lambda x: x[0] if x else None)
                # if area_results had more than 1 value add this to the next column
                for idx, areas in area_results.items():
                    if len(areas) > 1:
                        df.loc[idx, 'Rectangular_Area_2'] = areas[1]
            elif key in ['square_rod', 'round_square_single_rods_stock']:
                df['Square_Area_1'] = df[size_column].apply(
                    self.calculate_square_area)
        return updated_dataframes_dict
