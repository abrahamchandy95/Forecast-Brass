from inventory_calculation import CalculationManager, DataPreparer

from utils import get_column_by_keyword


class ExceptionManager:
    def __init__(
        self, config, live_sheets
    ) -> None:
        self.live_sheets = live_sheets
        self.calculation_manager = CalculationManager(config, live_sheets)
        self.data_preparer = DataPreparer(live_sheets)
        self.items_df = self.data_preparer.products_dataframe

    def mark_forged_products(self):
        self.calculation_manager.calculate_requirements()
        self.brass_requirements = self.calculation_manager.get_brass_requirements()
        scrap_dataframe = self.brass_requirements.get('Scrap')
        item_column = get_column_by_keyword(scrap_dataframe, 'item')
        if scrap_dataframe is not None:
            forged_products = scrap_dataframe[item_column].tolist()
            self.items_df['IsForged'] = self.items_df['Generic_Product_Code'].apply(
                lambda product: product in forged_products
            )

    def handle_unmatched_rows(self):
        df = self.items_df
        unmatched_criteria = df[self.data_preparer.lookup_columns].eq(
            0).all(axis=1)
        unmatched_rows = df[unmatched_criteria].copy()
        return unmatched_rows

    def execute(self):
        self.mark_forged_products()
        unmatched_rows = self.handle_unmatched_rows()
        return unmatched_rows
