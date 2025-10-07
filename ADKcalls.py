import logging
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.DEBUG)

def get_info(query: str) -> dict:
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        SERVICE_ACCOUNT_FILE = './instance/spreadsheet-certificate-046748612a86.json'
        SPREADSHEET_ID = '1O1czFagur4v2X6TQm-QKvwGc5RQpvzQJcysxMseCzHQ'
        # Dynamically get the first sheet's title
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get('sheets', '')
        SHEET_NAME = sheets[0].get('properties', {}).get('title', 'Sheet1') if sheets else 'Sheet1'

        sheet = service.spreadsheets()
        
        # Extract year from query
        year_match = re.search(r'\d{4}', query)
        year = year_match.group() if year_match else None
        if not year:
            return {"status": "error", "error_message": "Please specify a valid year (e.g., 2025)."}

        # First, fetch only the header row to get dynamic column names
        header_range = f'{SHEET_NAME}!1:1'  # Fetch only the first row for headers
        header_result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=header_range).execute()
        headers = header_result.get('values', [[]])[0]  # First row as headers
        if not headers:
            return {"status": "error", "error_message": "No headers found in the Google Sheet."}

        # Fetch only the row matching the year (assuming year is in column A)
        data_range = f'{SHEET_NAME}!A:A'  # Fetch column A to find the row
        data_result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=data_range).execute()
        values = data_result.get('values', [])

        row_index = None
        for i, row in enumerate(values, start=1):  # Start at 1 to match row numbers
            if len(row) > 0 and row[0] == year:
                row_index = i
                break

        if row_index is None:
            logging.debug(f"No data found for year: {year}")
            return {"status": "error", "error_message": f"No data found for year {year}."}

        # Fetch the full row data for the matched row
        row_range = f'{SHEET_NAME}!{row_index}:{row_index}'
        row_result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=row_range).execute()
        matching_row = row_result.get('values', [[]])[0]

        # Build a dynamic report with all columns
        report = {"year": year, "full_row": matching_row}
        for i, val in enumerate(matching_row):
            if i < len(headers):
                key = headers[i].lower().replace(' ', '_').replace('(', '').replace(')', '')
                report[key] = val if i < len(matching_row) else "N/A"

        # Determine the highlighted column based on query keywords
        query_lower = query.lower()
        highlighted_col = None
        for header in headers:
            if header.lower() in query_lower or any(k in query_lower for k in header.lower().split()):
                highlighted_col = header.lower().replace(' ', '_').replace('(', '').replace(')', '')
                break
        if highlighted_col and highlighted_col in report:
            report['highlighted'] = {highlighted_col: report[highlighted_col]}

        logging.debug(f"Found matching data for {year}: {report}")
        return {"status": "success", "report": report}

    except Exception as e:
        logging.error(f"Error accessing Google Sheet: {str(e)}")
        return {"status": "error", "error_message": f"An error occurred: {str(e)}"}
