from config import load_config
from data_processing import GoogleSheetsClient
from inventory_calculation import BrassStockRequirementsSummary


def main():
    config = load_config()
    try:
        google_sheets_client = GoogleSheetsClient(config)
        live_sheets = google_sheets_client.live_sheets

        brass_inventory_required = BrassStockRequirementsSummary(
            config, live_sheets)
        total_requirements = brass_inventory_required.find_total_requirements()
        print(total_requirements)
    except Exception as e:
        print(f'An error occurred: {e}')


if __name__ == '__main__':
    main()
