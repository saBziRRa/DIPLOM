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
OI_INTERVAL = "1h"           # Интервал для Open Interest: '5min', '15min', '30min', '1h', '4h', '1d'
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
        """Скачивает Open Interest с заданной даты до текущего момента."""
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
            
            logger.info(f"✓ Загружено {len(df)} записей Open Interest")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке Open Interest: {e}")
            raise
    
    def download_funding_rate(
        self,
        start_date: str
    ) -> pd.DataFrame:
        """Скачивает Funding Rate с заданной даты до текущего момента."""
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
            
            logger.info(f"✓ Загружено {len(df)} записей Funding Rate")
            
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
    logger.info(f"Интервал Open Interest: {OI_INTERVAL}")
    logger.info("="*70)
    
    try:
        downloader = BybitFuturesDataDownloader(symbol=SYMBOL, category=CATEGORY)
        
        # Загружаем Open Interest
        logger.info("\n[1/2] Загрузка Open Interest...")
        logger.info("-"*70)
        oi_df = downloader.download_open_interest(START_DATE, OI_INTERVAL)
        
        # Загружаем Funding Rate
        logger.info("\n[2/2] Загрузка Funding Rate...")
        logger.info("-"*70)
        fr_df = downloader.download_funding_rate(START_DATE)
        
        # Объединяем данные в один DataFrame
        logger.info("\n[3/3] Объединение данных...")
        logger.info("-"*70)
        
        if not oi_df.empty and not fr_df.empty:
            # Объединяем по timestamp (outer merge сохраняет все записи)
            combined_df = pd.merge(
                oi_df, 
                fr_df, 
                on='timestamp', 
                how='outer'
            )
            combined_df = combined_df.sort_values('timestamp')
            
            # Forward fill для funding_rate (заполняем пропуски предыдущим значением)
            combined_df['funding_rate'] = combined_df['funding_rate'].fillna(method='ffill')
            
            # Убираем строки где open_interest пустой (оставляем только интервалы OI)
            combined_df = combined_df.dropna(subset=['open_interest'])
            
        elif not oi_df.empty:
            combined_df = oi_df.copy()
            combined_df['funding_rate'] = None
            
        elif not fr_df.empty:
            combined_df = fr_df.copy()
            combined_df['open_interest'] = None
            
        else:
            logger.error("Не удалось загрузить данные")
            raise Exception("Нет данных для сохранения")
        
        # Сохраняем в один файл
        start_str = START_DATE.replace(':', '')
        end_str = datetime.datetime.now().strftime('%d%m%Y%H%M')
        output_file = f"futures_data_{SYMBOL}_{start_str}-{end_str}.csv"
        
        combined_df.to_csv(output_file, index=False)
        
        # Итоговая информация
        logger.info("\n" + "="*70)
        logger.info("✓ ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА")
        logger.info("="*70)
        logger.info(f"Файл: {output_file}")
        logger.info(f"Всего записей: {len(combined_df)}")
        logger.info(f"Open Interest записей: {combined_df['open_interest'].notna().sum()}")
        logger.info(f"Funding Rate записей: {combined_df['funding_rate'].notna().sum()}")
        logger.info("="*70)
        
        # Показываем превью данных
        logger.info("\nПервые 10 записей:")
        print(combined_df.head(10).to_string(index=False))
        
        logger.info("\nПоследние 5 записей:")
        print(combined_df.tail(5).to_string(index=False))
        
    except KeyboardInterrupt:
        logger.info("\n\n⚠ Прервано пользователем")
    except Exception as e:
        logger.error(f"\n\n❌ Критическая ошибка: {e}")
        raise
