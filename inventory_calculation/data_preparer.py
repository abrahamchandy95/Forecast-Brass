from data_modeling.products.open_orders import OrdersDataModeler
from utils import get_column_by_keyword, remove_textures


class DataPreparer(OrdersDataModeler):
    def __init__(self, live_sheets) -> None:
        super().__init__(live_sheets)
        self.shape_patterns = {
            'Circular': ('Circular_Area_{i}_Matched_FirstCol', 'Circular_Area_{i}_Matched_SecondCol', 'Cylinder_{i}_Volume'),
            'Rectangular': ('Rectangular_Area_{i}_Matched_FirstCol', 'Rectangular_Area_{i}_Matched_SecondCol', 'Cuboid_{i}_Volume'),
            'Square': ('Square_Area_1_Matched_FirstCol', 'Square_Area_1_Matched_SecondCol', 'Squared_1_Volume'),
            'Fixed': ('Matched_FirstCol', 'Matched_SecondCol', 'Sheet_Volume', 'IsForged')
        }
        self.ranges_for_shapes = {
            'Circular': range(1, 4),
            'Rectangular': range(1, 3),
            'Square': range(1, 2),
            'Fixed': range(1)
        }

        self.lookup_columns = []
        self.products_dataframe = self.orders_dataframe.copy()
        self.prepare_dataframe()

    def clean_and_prepare_data(self):
        pass

    def add_generic_product_name(self, df):
        df_copy = df.copy()
        item_column = get_column_by_keyword(df_copy, 'item')
        if item_column in df_copy.columns and 'Generic_Product_Code' not in df.columns:
            df_copy['Generic_Product_Code'] = df_copy[item_column].apply(
                remove_textures)
        return df_copy

    def initialize_lookup_columns(self):
        for shape, patterns in self.shape_patterns.items():
            range_for_i = self.ranges_for_shapes[shape]
            for i in range_for_i:
                columns = [p.format(i=i) for p in patterns] if shape != 'Fixed' else list(
                    patterns)
                self.lookup_columns.extend(columns)
        for column in self.lookup_columns:
            self.products_dataframe[column] = 0

    def prepare_dataframe(self):
        self.products_dataframe = self.add_generic_product_name(
            self.products_dataframe
        )
        self.initialize_lookup_columns()
