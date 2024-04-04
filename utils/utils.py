import re
from typing import Optional

import numpy as np
import pandas as pd

def convert_inches_to_mm(value_in_inches):
    """
    Converts a value from inches to millimeters
    """
    return value_in_inches * 25.4

def calculate_volume_from_weight(weight_in_kg, density=8.5):
    """
    Calculates the volume of an item using its weight
    """
    weight_in_grams = weight_in_kg * 1000
    volume_in_cubic_cm = weight_in_grams/ density
    return volume_in_cubic_cm

def remove_textures(product):
    """
    Removes anything categorized as a texture from a product

    Parameters:
    - product (str): the product or particular skew that is identified
    
    Returns:
    - base product: The base category or parent product without textures
    """
    string = str(product).replace('\'\'', '"').replace('-','')
    # Remove 'LH' or 'RH' at the end of each string
    # This rule is project specific
    string = re.sub(r'(LH|RH)$', '', string, flags=re.IGNORECASE)
    # identify the textures in products
    textures = 'LH|RH|KH|RT|H|K|B|R|T|HR|HL|ES|S|RR|RL|HO|HI|HA|NL|L'
    pattern = re.compile(f'(\d+)({textures})(?=\.\d+|$)', re.IGNORECASE)
    # Find all textures that follow the numeric part in a string
    # Keep only the numeric part
    string = pattern.sub(r'\1', string)
    return string.lower()

def get_column_by_keyword(df, keyword):
    """
    Identifies the first column in the DataFrame that contains a given keyword.

    Parameters:
    - df (pd.DataFrame): The DataFrame to search.
    - keyword (str): The keyword to search for in column names.

    Returns:
    - str: The name of the first column containing the keyword, or None
    """
    if df is not None and df.columns is not None:
        return next((col for col in df.columns if keyword.lower() in col.lower()), None)
    else:
        print(f'No {keyword} column found')
        return None

def calculate_rod_top_area(shape, dimension):
    #Extract the numeric part of the string
    numeric_part = float(re.findall(r'\d+\.?\d*', dimension)[0])
    if 'Round Rod' == shape:
        radius = numeric_part/2
        top_area = np.pi*(radius**2)
    elif 'Hex Rod' == shape:
        top_area = (3*np.sqrt(3)//2)*(numeric_part**2)
    elif 'Square Rod' == shape:
        top_area = numeric_part**2
    return top_area

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

def compute_volume(
    df: pd.DataFrame, area_column: str, height_column: str, volume_column: str
) -> pd.DataFrame:
    
    """Helper function to calculate volume if area and height columns are present."""
    df_copy = df.copy()
    # Ensure the area and height columns are numeric, convert non-numeric to NaN
    df_copy[area_column] = pd.to_numeric(df_copy[area_column], errors='coerce')
    df_copy[height_column] = pd.to_numeric(df_copy[height_column], errors='coerce')

    # Calculate volume only where both area and height columns have numeric values
    mask = df_copy[area_column].notna() & df_copy[height_column].notna()
    df_copy.loc[mask, volume_column] = df_copy.loc[mask, area_column] * df_copy.loc[mask, height_column]

    return df_copy

        
        