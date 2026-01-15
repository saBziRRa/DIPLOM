import pandas as pd
import requests
import datetime
import logging
from pathlib import Path
import time
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


# ============================================================================
# НАСТРОЙКИ - ИЗМЕНИТЕ ЗДЕСЬ
# ============================================================================
START_DATE = "01012024:0000"  # Формат: DDMMYYYY:HHMM
SYMBOL = "BTCUSDT"            # Торговая пара
CATEGORY = "linear"           # Тип контракта: 'linear' или 'inverse'
OI_INTERVAL = "1d"           # Интервал для Open Interest: '5min', '15min', '30min', '1h', '4h', '1d'

# Выберите что скачивать (True/False)
DOWNLOAD_OPEN_INTEREST = True
DOWNLOAD_FUNDING_RATE = True
# ============================================================================


class BybitFuturesDataDownloader:
    """Загрузчик данных по фьючерсам с Bybit API v5"""
    
    def __init__(self, symbol: str = "BTCUSDT", category: str = "linear"):
        self.symbol = symbol
        self.category = category
        self.base_url = "https://api.bybit.com"
    
    def _parse_timestamp(self, timestamp: str) -> int:
        """Конвертация формата DDMMYYYY:HHMM в Unix timestamp (миллисекунды)."""
        try:
            dt = datetime.datetime.strptime(timestamp, "%d%m%Y:%H%M")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logger.error(f"Неверный формат даты: {timestamp}. Ожидается: DDMMYYYY:HHMM")
            raise
    
    def download_open_interest(
        self,
        start_date: str,
        interval: str = "1h"
    ) -> pd.DataFrame:
        """
        Скачивает Open Interest с заданной даты до текущего момента.
        Сохраняет в файл open_interest_SYMBOL_dates.csv
        """
        try:
            start_time = self._parse_timestamp(start_date)
            end_time = int(datetime.datetime.now().timestamp() * 1000)
            
            logger.info(f"Загрузка Open Interest для {self.symbol}")
            logger.info(f"Период: {start_date} -> {datetime.datetime.now().strftime('%d%m%Y:%H%M')}")
            logger.info(f"Интервал: {interval}")
            
            endpoint = f"{self.base_url}/v5/market/open-interest"
            all_data = []
            cursor = None
            page = 0
            
            while True:
                page += 1
                params = {
                    "category": self.category,
                    "symbol": self.symbol,
                    "intervalTime": interval,
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": 200
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                logger.info(f"Страница {page}: запрос данных...")
                
                response = requests.get(endpoint, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data['retCode'] != 0:
                    raise Exception(f"Ошибка API: {data['retMsg']}")
                
                records = data['result']['list']
                next_cursor = data['result'].get('nextPageCursor', '')
                
                if not records:
                    logger.info("Больше данных нет")
                    break
                
                all_data.extend(records)
                logger.info(f"Получено {len(records)} записей | Всего: {len(all_data)}")
                
                if not next_cursor or next_cursor == cursor:
                    logger.info("Достигнут конец данных")
                    break
                
                cursor = next_cursor
                time.sleep(0.12)
            
            if not all_data:
                logger.warning("Данные не найдены за указанный период")
                return pd.DataFrame(columns=['timestamp', 'open_interest'])
            
            # Обработка данных
            df = pd.DataFrame(all_data)
            df['timestamp'] = df['timestamp'].astype(int)
            df['open_interest'] = df['openInterest'].astype(float)
            df = df[['timestamp', 'open_interest']]
            df = df.sort_values('timestamp')
            df = df.drop_duplicates(subset=['timestamp'])
            
            # Сохранение
            start_str = start_date.replace(':', '')
            end_str = datetime.datetime.now().strftime('%d%m%Y%H%M')
            output_file = f"open_interest_{self.symbol}_{start_str}-{end_str}.csv"
            
            df.to_csv(output_file, index=False)
            
            logger.info(f"✓ Сохранено {len(df)} записей в файл: {output_file}")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке Open Interest: {e}")
            raise
    
    def download_funding_rate(
        self,
        start_date: str
    ) -> pd.DataFrame:
        """
        Скачивает Funding Rate с заданной даты до текущего момента.
        Сохраняет в файл funding_rate_SYMBOL_dates.csv
        """
        try:
            start_time = self._parse_timestamp(start_date)
            end_time = int(datetime.datetime.now().timestamp() * 1000)
            
            logger.info(f"Загрузка Funding Rate для {self.symbol}")
            logger.info(f"Период: {start_date} -> {datetime.datetime.now().strftime('%d%m%Y:%H%M')}")
            
            endpoint = f"{self.base_url}/v5/market/funding/history"
            all_data = []
            cursor = None
            page = 0
            
            while True:
                page += 1
                params = {
                    "category": self.category,
                    "symbol": self.symbol,
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": 200
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                logger.info(f"Страница {page}: запрос данных...")
                
                response = requests.get(endpoint, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data['retCode'] != 0:
                    raise Exception(f"Ошибка API: {data['retMsg']}")
                
                records = data['result']['list']
                next_cursor = data['result'].get('nextPageCursor', '')
                
                if not records:
                    logger.info("Больше данных нет")
                    break
                
                all_data.extend(records)
                logger.info(f"Получено {len(records)} записей | Всего: {len(all_data)}")
                
                if not next_cursor or next_cursor == cursor:
                    logger.info("Достигнут конец данных")
                    break
                
                cursor = next_cursor
                time.sleep(0.12)
            
            if not all_data:
                logger.warning("Данные не найдены за указанный период")
                return pd.DataFrame(columns=['timestamp', 'funding_rate'])
            
            # Обработка данных
            df = pd.DataFrame(all_data)
            df['timestamp'] = df['fundingRateTimestamp'].astype(int)
            df['funding_rate'] = df['fundingRate'].astype(float)
            df = df[['timestamp', 'funding_rate']]
            df = df.sort_values('timestamp')
            df = df.drop_duplicates(subset=['timestamp'])
            
            # Сохранение
            start_str = start_date.replace(':', '')
            end_str = datetime.datetime.now().strftime('%d%m%Y%H%M')
            output_file = f"funding_rate_{self.symbol}_{start_str}-{end_str}.csv"
            
            df.to_csv(output_file, index=False)
            
            logger.info(f"✓ Сохранено {len(df)} записей в файл: {output_file}")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке Funding Rate: {e}")
            raise


if __name__ == "__main__":
    logger.info("="*70)
    logger.info("АВТОМАТИЧЕСКАЯ ЗАГРУЗКА ДАННЫХ ПО ФЬЮЧЕРСАМ BYBIT")
    logger.info("="*70)
    logger.info(f"Символ: {SYMBOL}")
    logger.info(f"Категория: {CATEGORY}")
    logger.info(f"Начальная дата: {START_DATE}")
    logger.info(f"Open Interest: {'ДА' if DOWNLOAD_OPEN_INTEREST else 'НЕТ'}")
    logger.info(f"Funding Rate: {'ДА' if DOWNLOAD_FUNDING_RATE else 'НЕТ'}")
    logger.info("="*70)
    
    try:
        downloader = BybitFuturesDataDownloader(symbol=SYMBOL, category=CATEGORY)
        
        results = {}
        
        # Загружаем Open Interest
        if DOWNLOAD_OPEN_INTEREST:
            logger.info("\n[1] Загрузка Open Interest...")
            logger.info("-"*70)
            oi_df = downloader.download_open_interest(START_DATE, OI_INTERVAL)
            results['open_interest'] = oi_df
            
            logger.info("\nПервые 10 записей Open Interest:")
            print(oi_df.head(10).to_string(index=False))
        
        # Загружаем Funding Rate
        if DOWNLOAD_FUNDING_RATE:
            logger.info("\n[2] Загрузка Funding Rate...")
            logger.info("-"*70)
            fr_df = downloader.download_funding_rate(START_DATE)
            results['funding_rate'] = fr_df
            
            logger.info("\nПервые 10 записей Funding Rate:")
            print(fr_df.head(10).to_string(index=False))
        
        # Итоговая информация
        logger.info("\n" + "="*70)
        logger.info("✓ ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА")
        logger.info("="*70)
        
        if 'open_interest' in results:
            logger.info(f"Open Interest: {len(results['open_interest'])} записей")
        
        if 'funding_rate' in results:
            logger.info(f"Funding Rate: {len(results['funding_rate'])} записей")
        
        logger.info("="*70)
        
    except KeyboardInterrupt:
        logger.info("\n\n⚠ Прервано пользователем")
    except Exception as e:
        logger.error(f"\n\n❌ Критическая ошибка: {e}")
        raise
