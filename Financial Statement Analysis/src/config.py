import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # API Configuration
    FMP_API_KEY = os.getenv('FMP_API_KEY')
    FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"
    
    # Companies to analyze
    COMPANIES = os.getenv('COMPANIES', 'AAPL,MSFT,GOOGL').split(',')
    YEARS_TO_FETCH = int(os.getenv('YEARS_TO_FETCH', '3'))
    
    # Paths
    RAW_DATA_PATH = "data/raw"
    PROCESSED_DATA_PATH = "data/processed"
    OUTPUT_PATH = "output"
    
    # Spark Configuration
    SPARK_APP_NAME = "FinancialAnalysis"
    SPARK_MASTER = "local[*]"

# Validate API key
if not Config.FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found! Please add it to your .env file")

print(f"Config loaded successfully. Analyzing {len(Config.COMPANIES)} companies.")