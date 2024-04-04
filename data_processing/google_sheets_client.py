import gspread
import pandas as pd
from google.oauth2.service_account import Credentials


class GoogleSheetsClient:
    def __init__(self, config):
        """
        Initializes the Google Sheets Client using credentials and sheet URL key.

        :param json_key_file_path: Path to the JSON key file for Google Sheets API authentication.
        :param url_key: Unique identifier for the Google Sheets to be accessed.
        """
        self.json_key_file_path = config.get(
            'GOOGLE_SHEETS_JSON_KEY_FILE_PATH')
        self.url_key = config.get('GOOGLE_SHEETS_URL_KEY')
        self.spreadsheet = None
        self.live_sheets = {}
        self._authorize_google_sheets()
        self._load_data_frames()

    def _authorize_google_sheets(self):
        """
        Authorizes this program to access Google Sheets
        """
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(
            self.json_key_file_path,
            scopes=scopes
        )
        gc = gspread.authorize(creds)
        self.spreadsheet = gc.open_by_key(self.url_key)

    def _load_data_frames(self):
        """
        Loads all the sheets as dataframes onto this program.
        Stores them in 'live_sheets' dictionary with sheet titles as keys
        """
        sheets = self.spreadsheet.worksheets()
        for sheet in sheets:
            data = sheet.get_all_values()
            df = pd.DataFrame(data)
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df.reset_index(drop=True, inplace=True)
            self.live_sheets[sheet.title] = df
