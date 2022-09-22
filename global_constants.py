from datetime import datetime

MAIN_DIR = 'D:\\Code\\Safegraph_Project'
DATA_DIR = MAIN_DIR + '\\SG_Data'

SPEND_PATTERNS = DATA_DIR + '\\spend_patterns'
TRANSACTION_PANEL_SUMMARY = DATA_DIR + '\\transaction_panel_summary'

CLEANED_DATA_DIR = DATA_DIR + '\\cleaned_data'

STAGING_1_INPUT = CLEANED_DATA_DIR + '\\staging_0_clean_data'

LOG_DIR = MAIN_DIR + f"\\Log\\{datetime.now().strftime('%Y%m%d')}"
LOG_FILE = LOG_DIR + f"\\Log_{datetime.now().strftime('%H%M%S')}.log"
