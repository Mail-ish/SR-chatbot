import json
import logging
from pathlib import Path
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from template_account_statement_service import TemplateAccountStatementService

logging.basicConfig(level=logging.INFO)

# Load config
CONFIG_PATH = Path(__file__).parent / "config.json"
with CONFIG_PATH.open() as f:
    config = json.load(f)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Sheets client
sheets_creds = Credentials.from_service_account_file(
    config["google_sheets"]["service_account_file"],
    scopes=SCOPES
)
sheets_service = build('sheets', 'v4', credentials=sheets_creds).spreadsheets()

# Drive client
drive_creds = Credentials.from_service_account_file(
    config["google_drive"]["service_account_file"],
    scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=drive_creds)

# Initialize service
template_service = TemplateAccountStatementService(
    sheets_client=sheets_service,
    drive_client=drive_service
)

# Test contract
contract_id = "SR230301034"
pdf_url = template_service.generate_single_statement(contract_id)

if pdf_url:
    print(f"PDF generated successfully: {pdf_url}")
else:
    print("PDF generation failed")
