import requests
import pandas as pd
import time
from typing import List, Dict
import os
from datetime import datetime, timedelta
from config import Config

class FMPClient:
    """FMP API client using only free tier endpoints"""
    
    def __init__(self):
        self.api_key = Config.FMP_API_KEY
        self.base_url = Config.FMP_BASE_URL
        self.session = requests.Session()
        
        # Create data directories
        os.makedirs(Config.RAW_DATA_PATH, exist_ok=True)
        os.makedirs(Config.PROCESSED_DATA_PATH, exist_ok=True)
        
    def _get(self, endpoint: str, params: Dict = None) -> Dict:
        """Make GET request to FMP API"""
        if params is None:
            params = {}
        params['apikey'] = self.api_key
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, params=params)
            if response.status_code == 403:
                return None
            response.raise_for_status()
            time.sleep(0.3)  # Rate limiting
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {endpoint}: {e}")
            return None
    
    def get_company_profile(self, symbol: str) -> pd.DataFrame:
        """Get company profile"""
        data = self._get(f"profile/{symbol}")
        if data:
            return pd.DataFrame(data)
        return pd.DataFrame()
    
    def get_quote(self, symbol: str) -> pd.DataFrame:
        """Get real-time quote"""
        data = self._get(f"quote/{symbol}")
        if data:
            return pd.DataFrame(data)
        return pd.DataFrame()
    
    def get_historical_prices(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """Get historical daily prices (limited history)"""
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        params = {
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d")
        }
        
        data = self._get(f"historical-price-full/{symbol}", params)
        if data and 'historical' in data:
            df = pd.DataFrame(data['historical'])
            df['symbol'] = symbol
            return df
        return pd.DataFrame()
    
    def get_market_indexes(self) -> pd.DataFrame:
        """Get major market indexes"""
        indexes = ["^GSPC", "^DJI", "^IXIC"]  # S&P 500, Dow Jones, NASDAQ
        all_data = []
        
        for index in indexes:
            data = self._get(f"quote/{index}")
            if data:
                all_data.extend(data)
        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def fetch_all_data(self, symbols: List[str] = None):
        """Fetch all available free data"""
        if symbols is None:
            symbols = Config.COMPANIES
            
        print(f"Starting data collection for {len(symbols)} companies")
        print("Available data: Company profiles, quotes, and historical prices")
        
        all_profiles = []
        all_quotes = []
        all_historical = []
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] Fetching data for {symbol}...")
            
            try:
                # Company profile
                profile = self.get_company_profile(symbol)
                if not profile.empty:
                    all_profiles.append(profile)
                    print(f"  ✓ Company profile fetched")
                
                # Current quote
                quote = self.get_quote(symbol)
                if not quote.empty:
                    all_quotes.append(quote)
                    print(f"  ✓ Current quote: ${quote['price'].iloc[0]:.2f}")
                
                # Historical prices (1 year)
                historical = self.get_historical_prices(symbol, days=365)
                if not historical.empty:
                    all_historical.append(historical)
                    print(f"  ✓ Historical prices: {len(historical)} days")
                
            except Exception as e:
                print(f"  ✗ Error fetching {symbol}: {e}")
        
        # Get market indexes
        print("\nFetching market indexes...")
        market_indexes = self.get_market_indexes()
        if not market_indexes.empty:
            market_indexes.to_csv(f"{Config.RAW_DATA_PATH}/market_indexes.csv", index=False)
            print(f"✓ Market indexes saved")
        
        # Save data
        print("\n" + "="*50)
        print("Saving data...")
        
        if all_profiles:
            profiles_df = pd.concat(all_profiles, ignore_index=True)
            profiles_df.to_csv(f"{Config.RAW_DATA_PATH}/company_profiles.csv", index=False)
            print(f"✓ Saved {len(profiles_df)} company profiles")
        
        if all_quotes:
            quotes_df = pd.concat(all_quotes, ignore_index=True)
            quotes_df.to_csv(f"{Config.RAW_DATA_PATH}/current_quotes.csv", index=False)
            print(f"✓ Saved {len(quotes_df)} current quotes")
        
        if all_historical:
            historical_df = pd.concat(all_historical, ignore_index=True)
            historical_df.to_csv(f"{Config.RAW_DATA_PATH}/historical_prices.csv", index=False)
            print(f"✓ Saved {len(historical_df)} historical price records")
        
        print(f"\nData collection complete!")
        
        return {
            'profiles': len(profiles_df) if all_profiles else 0,
            'quotes': len(quotes_df) if all_quotes else 0,
            'historical_records': len(historical_df) if all_historical else 0
        }

if __name__ == "__main__":
    client = FMPClient()
    summary = client.fetch_all_data()
    print("\nSummary:", summary)