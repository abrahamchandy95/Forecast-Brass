from typing import Dict, Tuple
import re

import pandas as pd

from data_processing.constants import STOCK_TO_UNITS_MAP
from utils import convert_inches_to_mm


class DescriptionDimensionProcessor:
    def __init__(self, units_dict: Dict[str, str] = None) -> None:
        if units_dict is None:
            units_dict = STOCK_TO_UNITS_MAP
        self.units_dicts = units_dict

    def parse_dimensions_from_rod_description(
        self, rod_description: str, units_map_dict: Dict[str, str]
    ) -> str:
        """
        Extracts the Rod's dimensions from its description in inventory onhand

        Parameters:
            rod_description (str): The string containing dimension information
            units_map_dict (dict): A dictionary mapping rod type to dimension

        Returns:
            str: A string that contains brief rod description and dimension 
        """
        dimension_string = rod_description
        units_of_measurement = None
        for indicator, units in units_map_dict.items():
            if indicator.lower() in dimension_string.lower():
                dimension_string = re.sub(
                    re.escape(indicator), '', dimension_string, flags=re.IGNORECASE
                ).strip()
                units_of_measurement = units
                break
        if units_of_measurement:
            dimension_values = re.findall(r'\d+\.?\d*', dimension_string)
            if dimension_values:
                highest_value = max(map(float, dimension_values))
                return f'{highest_value}{units_of_measurement}'
        return rod_description.strip()

    def add_rod_dimensions_to_dataframe(
            self, df: pd.DataFrame, dimension_column: str
    ) -> pd.DataFrame:
        """
        Adds a column to a dataframe that tells you the dimensions of each rod

        Parameters
        - df (DataFrame): The DataFrame containing rods
        - dimension_column: The column that contains the description of the rod

        Returns
        - The dataframe with an added column that has the diameter
        """
        df_copy = df.copy()
        df_copy.loc[:, dimension_column] = df[dimension_column].apply(
            lambda x: self.parse_dimensions_from_rod_description(
                x, self.units_dicts)
        )
        return df_copy

    def extract_and_standardize_dimensions(
            self, dimension_string: str
    ) -> Tuple[float, float]:
        """
        Extracts dimensions from a strings and converts them to millimeters

        Parameters
        - dimension_string (str): The description of rod containing dimensions.

        Returns
        - The value of the dimensions in millimeters
        """
        if pd.isna(dimension_string):
            return [0.0, 0.0]  # Return default values for missing values
        dimension_string = str(dimension_string).strip()
        # Initialize a list to store the dimensions
        stored_dimensions = []
        standardized_string = dimension_string.replace('\'\'', '"').strip()
        # finds three groups - numeric, '"' and mm/MM
        matches = re.findall(
            r'(\d+\.?\d*)\s*(")?\s*(mm|MM)?', standardized_string)
        for value, inch_indicator, _ in matches:
            value = float(value)
            if '"' == inch_indicator:
                value = convert_inches_to_mm(value)
            stored_dimensions.append(value)  # Add extracted dimensions here
        # Use the 2 largest dimensions
        largest_dimensions = sorted(stored_dimensions, reverse=True)[:2]
        return [round(dimension, 2) for dimension in largest_dimensions]

    def add_non_rod_dimensions_to_dataframe(
            self, df: pd.DataFrame, dimensions_column: str
    ) -> pd.DataFrame:
        """
        Extracts dimensions from each row within a dataframe,
        Adds them to separate columns. Not for rod data

        Parameters
        - df (pd.DataFrame): The dataframe that we must add dimensions to
        - dimension_column (str): The column that contains the dimensions 
        to be parsed

        Returns
        A dataframe with 2 new columns containing new dimensions
        """
        df_copy = df.copy()
        dimensions = df_copy[dimensions_column].apply(
            self.extract_and_standardize_dimensions
        )
        df_copy['Dimension_1'] = dimensions.apply(
            lambda x: round(x[0], 2) if len(x) > 0 else 0.0
        )
        df_copy['Dimension_2'] = dimensions.apply(
            lambda x: round(x[1], 2) if len(x) > 1 else 0.0
        )
        return df_copy