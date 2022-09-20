from datetime import datetime

MAIN_DIR = 'D:\\Code\\Safegraph_Project'
DATA_DIR = MAIN_DIR + '\\SG_Data'

SPEND_PATTERNS = DATA_DIR + '\\spend_patterns'
TRANSACTION_PANEL_SUMMARY = DATA_DIR + '\\transaction_panel_summary'

CLEANED_DATA_DIR = DATA_DIR + '\\cleaned_data'

STAGING_0_INPUT = CLEANED_DATA_DIR + '\\staging_0_clean_data'

STAGING_1_INPUT = CLEANED_DATA_DIR + '\\staging_1_naics_list'
STAGING_1_OUTPUT = STAGING_1_INPUT + '\\NAICS_LIST'

STAGING_2_OUTPUT = CLEANED_DATA_DIR + '\\staging_2_naics_view'

STAGING_3_INPUT = CLEANED_DATA_DIR + '\\staging_3_filter_view'

LOG_DIR = MAIN_DIR + f"\\Log\\{datetime.now().strftime('%Y%m%d')}"
LOG_FILE = LOG_DIR + f"\\Log_{datetime.now().strftime('%H%M%S')}.log"