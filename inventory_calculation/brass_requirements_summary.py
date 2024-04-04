from typing import Dict

import pandas as pd

from inventory_calculation import CalculationManager, DataPreparer
from utils import get_column_by_keyword


class BrassStockRequirementsSummary:
    def __init__(self, config, live_sheets) -> None:
        self.data_preparer = DataPreparer(live_sheets)
        self.items_df: pd.DataFrame = self.data_preparer.products_dataframe.copy()
        self.calculation_manager: CalculationManager = CalculationManager(config, live_sheets)
        self.calculation_manager.calculate_requirements()
        self.brass_requirements: Dict[
            str, pd.DataFrame
        ] = self.calculation_manager.get_brass_requirements()
        self.tally_brass_requirements_per_product()
        self.aggregated_results: pd.DataFrame = self.aggregate_volumes()
        self.stacked_dataframe: pd.DataFrame = self.stack_columns()

    def tally_brass_requirements_per_product(self) -> None:
        """
        Maps required brass inventory onto each product
        """
        for _, original_dataframe in self.brass_requirements.items():
            dataframe = original_dataframe.copy()
            self.data_preparer.add_generic_product_name(dataframe)
            for _, required_row in dataframe.iterrows():
                item = required_row['Generic_Product_Code']
                matches = self.items_df[self.items_df['Generic_Product_Code'] == item].index
                for column in dataframe.columns:
                    if column in self.items_df.columns and column != 'Generic_Product_Code':
                        self.items_df.loc[
                            matches, column] = required_row[column]

    def generate_volume_mapping(self) -> Dict[tuple, str]:
        """
        Helper function for the aggregate_volumes function
        helps with dynamically adding all necessary columns
        """
        volume_info: Dict[str, tuple] = {
            'Circular': ('Cylinder', range(1, 4)),
            'Rectangular': ('Cuboid', range(1, 3)),
            'Square': ('Squared', range(1, 2))
        }
        volume_mapping: Dict[tuple, str] = {}
        for shape, (volume_type, shape_range) in volume_info.items():
            for i in shape_range:
                key = (
                    f'{shape}_Area_{i}_Matched_FirstCol', f'{shape}_Area_{i}_Matched_SecondCol'
                )
                value = f'{volume_type}_{i}_Volume'
                volume_mapping[key] = value
        volume_mapping[('Matched_FirstCol', 'Matched_SecondCol')] = 'Sheet_Volume'
        return volume_mapping

    def aggregate_volumes(self) -> pd.DataFrame:
        volume_mapping: Dict[tuple, str] = self.generate_volume_mapping()
        aggregated_results_list: list = []
        for columns, volume_column in volume_mapping.items():
            concatenated_column = '_and_'.join(columns)
            self.items_df[concatenated_column] = self.items_df[list(columns)].apply(
                lambda row: ' '.join(row.values.astype(str)), axis=1
            )
            qty_column = get_column_by_keyword(self.items_df, 'qty')

            if qty_column in self.items_df.columns:
                self.items_df[volume_column] *= self.items_df[qty_column]
            summed_volume_df = self.items_df.groupby(
                concatenated_column, as_index=False).agg({volume_column: 'sum'})
            summed_volume_df.rename(
                columns={volume_column: f'Sum_{volume_column}'}, inplace=True)
            aggregated_results_list.append(summed_volume_df)
        all_results_combined = pd.concat(aggregated_results_list, axis=1)
        non_duplicated_columns = ~all_results_combined.columns.duplicated()
        aggregated_results = all_results_combined.loc[
            :, non_duplicated_columns
        ]
        return aggregated_results

    def stack_columns(self) -> pd.DataFrame:
        stacked_dataframe: pd.DataFrame = pd.DataFrame(
            columns=['Stock Type', 'Volume'])
        for i in range(0, len(self.aggregated_results.columns), 2):
            column1 = self.aggregated_results.columns[i]
            column2 = self.aggregated_results.columns[i + 1] if i + 1 < len(
                self.aggregated_results.columns) else None

            if column2 is not None:
                # Extracting rows that have a non-zero volume
                non_zero_rows = self.aggregated_results[self.aggregated_results[column2] > 0]
                temp_df = non_zero_rows[[column1, column2]].dropna().rename(
                    columns={column1: 'Stock Type', column2: 'Volume'})
                stacked_dataframe = pd.concat(
                    [stacked_dataframe, temp_df], ignore_index=True)

        return stacked_dataframe

    def find_total_requirements(self) -> pd.DataFrame:
        grouped_dataframe = self.stacked_dataframe.groupby(
            'Stock Type')['Volume'].sum().reset_index()
        grouped_dataframe['Volume (cm^3)'] = grouped_dataframe['Volume'].astype(
            float) / 1000
        grouped_dataframe['Weight (kg)'] = (
            grouped_dataframe['Volume (cm^3)'] * 8.5) / 1000
        return grouped_dataframe
