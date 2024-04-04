from product_profile_calculator import ProfileCalculator


class CalculationManager:
    def __init__(self, config, live_sheets) -> None:
        """Initialize the CalculationManager with a ProfileCalculator instance."""
        self.live_sheets = live_sheets
        self.profile_calculator = ProfileCalculator(config, live_sheets)
        self.brass_requirements = {}
        
    def calculate_requirements(self):
        """
        Executes the profile calculation workflow to calculate brass requirements
        for each product.
        """
        self.brass_requirements = self.profile_calculator.execute_workflow()
        
    def get_brass_requirements(self):
        return self.brass_requirements
        