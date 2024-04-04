from abc import ABC, abstractmethod

from data_processing import(
    SupplyChainDataPrep,
    DescriptionDimensionProcessor
)


class BaseDataModeler(ABC):
    """
    Abstract base class for data modelers.
    Initializes necessary data processing components with provided configuration.
    """
    def __init__(self, live_sheets):
       
        self.live_sheets = live_sheets
        self.supply_chain_data_prep = SupplyChainDataPrep(
            live_sheets=self.live_sheets
        )
        self.description_dimension_processor = DescriptionDimensionProcessor()

    @abstractmethod
    def load_data_frame(self):
        pass

    @abstractmethod
    def clean_and_prepare_data(self):
        pass

    