import pandas as pd
import requests
import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# НАСТРОЙКИ 
# ============================================================================
START_DATE = "01012024:0000"  # Формат: DDMMYYYY:HHMM
FG_INTERVAL = "1d"            # Варианты: '4h' (4 часа) или '1d' (день)
                               # '4h' - интерполирует дневные данные на 4-часовые интервалы
                               # '1d' - оригинальные дневные данные
# ============================================================================


class FearGreedDownloader:
    """Загрузчик Fear & Greed Index"""
    
    def __init__(self):
        self.api_url = "https://api.alternative.me/fng/?limit=0"
    
    def _parse_timestamp(self, timestamp: str) -> int:
        """Конвертация формата DDMMYYYY:HHMM в Unix timestamp (миллисекунды)."""
        try:
            dt = datetime.datetime.strptime(timestamp, "%d%m%Y:%H%M")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logger.error(f"Неверный формат даты: {timestamp}. Ожидается: DDMMYYYY:HHMM")
            raise
    
    def _resample_to_4h(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Преобразует дневные данные в 4-часовые интервалы.
        Использует forward fill для заполнения промежуточных значений.
        """
        logger.info("Преобразование в 4-часовые интервалы...")
        
        df['dt'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('dt')

        start_time = df.index.min().floor('4H')
        end_time = df.index.max().ceil('4H')
        
        full_range = pd.date_range(start=start_time, end=end_time, freq='4H')
        
        df_resampled = df.reindex(full_range, method='ffill')

        df_resampled = df_resampled.reset_index()
        df_resampled = df_resampled.rename(columns={'index': 'dt'})
        df_resampled['timestamp'] = (df_resampled['dt'].astype(int) / 1e6).astype(int)

        df_resampled = df_resampled[['timestamp', 'fear_greed_index']]
        
        logger.info(f"Создано {len(df_resampled)} 4-часовых записей из {len(df)} дневных")
        
        return df_resampled
    
    def download_fear_greed_index(
        self, 
        start_date: str,
        interval: str = "1d"
    ) -> pd.DataFrame:
        """
        Скачивает Crypto Fear & Greed Index с заданной даты до текущего момента.
        Сохраняет в файл fear_greed_index_INTERVAL_dates.csv
        
        Args:
            start_date: Начальная дата в формате DDMMYYYY:HHMM
            interval: Интервал '1d' (день) или '4h' (4 часа)
        """
        try:
            start_time = self._parse_timestamp(start_date)
            
            logger.info("Загрузка Fear & Greed Index")
            logger.info(f"Период: {start_date} -> {datetime.datetime.now().strftime('%d%m%Y:%H%M')}")
            logger.info(f"Интервал: {interval}")
            logger.info("Запрос данных из API...")
            response = requests.get(self.api_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()["data"]
            
            if not data:
                logger.warning("API не вернул данных")
                return pd.DataFrame(columns=['timestamp', 'fear_greed_index'])

            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
            df["timestamp"] = df["timestamp"] * 1000  # секунды -> миллисекунды
            df["timestamp"] = df["timestamp"].astype(int)
            
            df = df[df["timestamp"] >= start_time]
            
            df = df[["timestamp", "value"]].rename(columns={"value": "fear_greed_index"})
            df["fear_greed_index"] = df["fear_greed_index"].astype(int)
            
            df = df.sort_values('timestamp')
            df = df.drop_duplicates(subset=['timestamp'])
            
            if df.empty:
                logger.warning(f"Нет данных за период с {start_date}")
                return df

            if interval == "4h":
                df = self._resample_to_4h(df)

            start_str = start_date.replace(':', '')
            end_str = datetime.datetime.now().strftime('%d%m%Y%H%M')
            output_file = f"fear_greed_index_{interval}_{start_str}-{end_str}.csv"
            
            df.to_csv(output_file, index=False)
            
            logger.info(f" Сохранено {len(df)} записей в файл: {output_file}")

            min_ts = pd.to_datetime(df['timestamp'].min(), unit='ms')
            max_ts = pd.to_datetime(df['timestamp'].max(), unit='ms')
            logger.info(f"Период данных: {min_ts} -> {max_ts}")
            logger.info(f"Диапазон индекса: {df['fear_greed_index'].min()} - {df['fear_greed_index'].max()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке Fear & Greed Index: {e}")
            raise


if __name__ == "__main__":
    logger.info("="*70)
    logger.info("АВТОМАТИЧЕСКАЯ ЗАГРУЗКА FEAR & GREED INDEX")
    logger.info("="*70)
    logger.info(f"Начальная дата: {START_DATE}")
    logger.info(f"Интервал: {FG_INTERVAL}")
    logger.info("="*70)
    
    try:
        downloader = FearGreedDownloader()

        logger.info("\nЗагрузка Fear & Greed Index...")
        logger.info("-"*70)
        fg_df = downloader.download_fear_greed_index(START_DATE, FG_INTERVAL)

        logger.info("\n" + "="*70)
        logger.info("✓ ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА")
        logger.info("="*70)
        logger.info(f"Fear & Greed Index ({FG_INTERVAL}): {len(fg_df)} записей")
        logger.info("="*70)

        if not fg_df.empty:
            logger.info("\nПервые 10 записей:")
            print(fg_df.head(10).to_string(index=False))
            
            logger.info("\nПоследние 5 записей:")
            print(fg_df.tail(5).to_string(index=False))
        
    except KeyboardInterrupt:
        logger.info("\n\n⚠ Прервано пользователем")
    except Exception as e:
        logger.error(f"\n\n Критическая ошибка: {e}")
        raise
