import logging
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import Dict, Optional

logging.basicConfig(level=logging.DEBUG)

def get_info(query: str) -> Dict[str, str]:
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        SERVICE_ACCOUNT_FILE = './instance/spreadsheet-certificate-046748612a86.json' # main.py
        # SERVICE_ACCOUNT_FILE = './spreadsheet-certificate-046748612a86.json' # Bol7API.py

        SPREADSHEET_ID = '1O1czFagur4v2X6TQm-QKvwGc5RQpvzQJcysxMseCzHQ' # bol7
        # SPREADSHEET_ID = '14XZFGM8UN8DDga7dH30t8ycYjHeGG-w9gDk_5hI8rns' # ankur

        # Initialize credentials and service
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)

        # Get sheet metadata to find the first sheet's name
        sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get('sheets', '')
        SHEET_NAME = sheets[0].get('properties', {}).get('title', 'Sheet1') if sheets else 'Sheet1'

        # Get all data from the sheet
        range_name = f'{SHEET_NAME}!A:Z'  # Adjust range as needed for your sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()

        values = result.get('values', [])
        if not values:
            logging.error("No data found in the spreadsheet")
            return {}

        # Get headers from the first row
        headers = values[0]
        
        # Search for the query in the first column
        for row in values[1:]:  # Skip header row
            if row and len(row) > 0 and str(row[0]).lower() == query.lower():
                # Create dictionary mapping headers to row values
                row_data = {}
                for i, value in enumerate(row):
                    header = headers[i] if i < len(headers) else f'Column_{i+1}'
                    row_data[header] = value
                return row_data

        logging.warning(f"No row found matching query: {query}")
        return {}

    except Exception as e:
        logging.error(f"Error fetching spreadsheet data: {str(e)}")
        return {}

# # Example usage
# if __name__ == "__main__":
#     # Example query to find a row where the first column matches this value
#     query = "Microsoft Office Home and Business 2024 Mac-Win"
#     # query = "2028"
#
#     result = get_info(query)
#     if result:
#         print("Found row data:")
#         for key, value in result.items():
#             print(f"{key}: {value}")
#     else:
#         print(f"No data found for query: {query}")
