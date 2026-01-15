import pandas as pd
import requests
import datetime
import logging
from pathlib import Path
import time
from typing import Optional
import sys
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ByBitDataManager:
    def __init__(self, base_file: str = "bybit_daily.csv"):
        self.base_file = base_file
        self.base_url = "https://api.bybit.com/v5/market/kline"
        self.symbol = "BTCUSDT"
        self.interval = "D"
        
    def _parse_timestamp(self, timestamp: str) -> int:
        """Convert DDMMYYYY:HHMM format to Unix timestamp in milliseconds."""
        try:
            dt = datetime.datetime.strptime(timestamp, "%d%m%Y:%H%M")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logger.error(f"Invalid timestamp format: {timestamp}. Expected format: DDMMYYYY:HHMM")
            raise

    def _format_timestamp(self, timestamp_ms: int) -> str:
        """Convert Unix timestamp in milliseconds to DDMMYYYY:HHMM format."""
        dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000)
        return dt.strftime("%d%m%Y:%H%M")

    def _get_latest_timestamp(self) -> Optional[int]:
        """Get the latest timestamp from the base CSV file."""
        try:
            if Path(self.base_file).exists():
                df = pd.read_csv(self.base_file)
                if not df.empty:
                    return int(df['timestamp'].max())
            return None
        except Exception as e:
            logger.error(f"Error reading base file: {e}")
            return None

    def _probe_timestamp(self, timestamp: int) -> bool:
        """Probe if data exists at the given timestamp."""
        try:
            params = {
                "category": "linear",
                "symbol": self.symbol,
                "interval": self.interval,
                "start": timestamp,
                "end": timestamp + 86400000,  # Check next day (24 часа)
                "limit": 1
            }
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['retCode'] != 0:
                raise Exception(f"API Error: {data['retMsg']}")
            
            return len(data['result']['list']) > 0
        except Exception as e:
            logger.error(f"Error probing timestamp: {e}")
            return False

    def _get_earliest_timestamp(self) -> int:
        """Find the earliest available timestamp for the symbol using binary search-like probing."""
        try:
            current_year = datetime.datetime.now().year
            earliest_year = 2021
            
            earliest_found_year = None
            year = current_year
            while year >= earliest_year:
                start_of_year = int(datetime.datetime(year, 1, 1).timestamp() * 1000)
                if self._probe_timestamp(start_of_year):
                    logger.info(f"Found data in year {year}")
                    if year > earliest_year:
                        prev_year_start = int(datetime.datetime(year - 1, 1, 1).timestamp() * 1000)
                        if self._probe_timestamp(prev_year_start):
                            logger.info(f"Found data in previous year {year - 1}, continuing search")
                            year -= 1
                            continue
                    earliest_found_year = year
                    break
                year -= 1
                time.sleep(0.1)
            
            if earliest_found_year is None:
                raise Exception("No data found before 2018")
            
            logger.info(f"Earliest year with data: {earliest_found_year}")
            earliest_found_month = None
            for month in range(1, 13):
                start_of_month = int(datetime.datetime(earliest_found_year, month, 1).timestamp() * 1000)
                if self._probe_timestamp(start_of_month):
                    logger.info(f"Found data in month {month} of {earliest_found_year}")
                    earliest_found_month = month
                    break
                else:
                    logger.info(f"No data in month {month} of {earliest_found_year}")
                time.sleep(0.1)
            
            if earliest_found_month is None:
                raise Exception(f"No data found in any month of {earliest_found_year}")
            
            logger.info(f"Earliest month with data: {earliest_found_month} in {earliest_found_year}")

            earliest_found_day = None
            for day in range(1, 32):
                try:
                    start_of_day = int(datetime.datetime(earliest_found_year, earliest_found_month, day).timestamp() * 1000)
                    if self._probe_timestamp(start_of_day):
                        logger.info(f"Found data on day {day} of month {earliest_found_month} in {earliest_found_year}")
                        earliest_found_day = day
                        break
                    else:
                        logger.info(f"No data in day {day} of month {earliest_found_month} in {earliest_found_year}")
                except ValueError:
                    continue
                time.sleep(0.1)
            
            if earliest_found_day is None:
                raise Exception(f"No data found in any day of month {earliest_found_month} in {earliest_found_year}")
            
            logger.info(f"Earliest day with data: {earliest_found_day} in {earliest_found_month}/{earliest_found_year}")
            
            start_of_day = int(datetime.datetime(earliest_found_year, earliest_found_month, earliest_found_day).timestamp() * 1000)
            return start_of_day
        
        except Exception as e:
            logger.error(f"Error finding earliest timestamp: {e}")
            raise

    def _fetch_data(self, start_time: int, end_time: int) -> pd.DataFrame:
        """Fetch data from ByBit API."""
        try:
            params = {
                "category": "linear",
                "symbol": self.symbol,
                "interval": self.interval,
                "start": start_time,
                "end": end_time,
                "limit": 1000
            }
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['retCode'] != 0:
                raise Exception(f"API Error: {data['retMsg']}")
            
            klines = data['result']['list']
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 
                'volume', 'turnover'
            ])
            df['timestamp'] = df['timestamp'].astype(int)
            return df
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing API response: {e}")
            raise

    def fetch(self) -> None:
        """Fetch new data and update the base CSV file."""
        try:
            latest_timestamp = self._get_latest_timestamp()
            current_time = int(datetime.datetime.now().timestamp() * 1000)
            
            if latest_timestamp is None:
                start_time = self._get_earliest_timestamp()
                logger.info("No existing data found. Starting from the beginning of ByBit history.")
            else:
                # Для дневных данных прибавляем ровно сутки (вместо 60000 мс = 1 минута)
                start_time = latest_timestamp + 86400000  
            
            if start_time >= current_time:
                logger.info("No new data to fetch")
                return
            
            while start_time < current_time:
                end_time = min(start_time + (1000 * 86400000), current_time)  
                logger.info(f"Fetching daily data from {datetime.datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d')} "
                            f"to {datetime.datetime.fromtimestamp(end_time/1000).strftime('%Y-%m-%d')}")
                
                df = self._fetch_data(start_time, end_time)
                if not df.empty:
                    logger.info(f"Added {len(df)} daily candles")
                    
                    if Path(self.base_file).exists():
                        existing_data = pd.read_csv(self.base_file)
                        combined_data = pd.concat([existing_data, df], ignore_index=True)
                        combined_data = combined_data.drop_duplicates(subset=['timestamp'])
                        combined_data = combined_data.sort_values('timestamp')
                    else:
                        combined_data = df
                    
                    combined_data.to_csv(self.base_file, index=False)
                    logger.info(f"Successfully updated {self.base_file} with {len(df)} new records")
                else:
                    logger.warning(f"No data found for period {datetime.datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d')} "
                                   f"to {datetime.datetime.fromtimestamp(end_time/1000).strftime('%Y-%m-%d')}")

                start_time = end_time + 86400000  
                time.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error in fetch operation: {e}")
            raise

    def get(self, start_time: str, end_time: str) -> str:
        """Get data for a specific time range and save to a new CSV file."""
        try:
            start_ts = self._parse_timestamp(start_time)
            end_ts = self._parse_timestamp(end_time)
            
            if not Path(self.base_file).exists():
                raise FileNotFoundError(f"Base file {self.base_file} not found. Please run fetch first.")
            
            df = pd.read_csv(self.base_file)
            df['timestamp'] = df['timestamp'].astype(int)
            
            mask = (df['timestamp'] >= start_ts) & (df['timestamp'] <= end_ts)
            filtered_df = df[mask]
            
            if filtered_df.empty:
                raise ValueError(f"No data found for the specified time range: {start_time} to {end_time}")
            
            output_file = f"dataset_daily_{start_time}--{end_time}.csv"
            filtered_df.to_csv(output_file, index=False)
            logger.info(f"Successfully created {output_file} with {len(filtered_df)} records")
            
            return output_file
            
        except Exception as e:
            logger.error(f"Error in get operation: {e}")
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ByBit Daily BTC/USDT Data Manager')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    fetch_parser = subparsers.add_parser('fetch', help='Fetch new daily data from ByBit')
    get_parser = subparsers.add_parser('get', help='Get daily data for a specific time range')
    get_parser.add_argument('start_time', help='Start time in DDMMYYYY:HHMM format')
    get_parser.add_argument('end_time', help='End time in DDMMYYYY:HHMM format')

    args = parser.parse_args()

    manager = ByBitDataManager()

    if args.command == 'fetch':
        try:
            manager.fetch()
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            sys.exit(1)
    elif args.command == 'get':
        try:
            output_file = manager.get(args.start_time, args.end_time)
            print(f"Data saved to: {output_file}")
        except Exception as e:
            logger.error(f"Failed to get data: {e}")
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)
