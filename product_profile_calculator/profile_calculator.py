from product_profile_calculator import (
    ProductAreaCalculator, ProductSourceLinker, ProductVolumeCalculator
)
from data_modeling.products.product_manufacturing_data import (
    DimensionUpdater
)
from data_modeling.raw_materials import BrassStockModeler


class ProfileCalculator():
    def __init__(self, config, live_sheets):
        self.live_sheets = live_sheets
        self.brass_stock_modeler = BrassStockModeler(live_sheets)
        self.dimension_updater = DimensionUpdater(config)
        self.area_calculator = ProductAreaCalculator()
        self.source_linker = ProductSourceLinker()
        self.volume_calculator = ProductVolumeCalculator(self.area_calculator)
        self.dimension_updater.update_dimensions_with_hardcoded_data()

    def execute_workflow(self):
        items_dict = self.dimension_updater.product_engineering_categories
        raw_stock_dict = self.brass_stock_modeler.inventory_dict
        processed_items_dict = self.area_calculator.parse_circular_areas_into_dict(
            items_dict)
        processed_items_dict = self.area_calculator.calculate_areas_for_rectangular_shapes(
            processed_items_dict)

        # Match raw material inventory
        linked_items_dict = self.source_linker.lookup_raw_stock(
            processed_items_dict, raw_stock_dict
        )
        # Calculate the material requirement from the raw stock
        brass_requirements = self.volume_calculator.calculate_cylinder_volume(
            linked_items_dict
        )
        brass_requirements = self.volume_calculator.calculate_cuboid_volume(
            brass_requirements
        )
        if 'metal_sheet' in brass_requirements:
            brass_requirements['metal_sheet'] = self.volume_calculator.calculate_sheet_volume(
                brass_requirements['metal_sheet']
            )
        return brass_requirements
