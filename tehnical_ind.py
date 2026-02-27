import pandas as pd
import numpy as np
from pybit.unified_trading import HTTP
from datetime import datetime
import ta  

def download_and_compute_technical_indicators(symbols=['BTCUSDT', 'ETHUSDT'], 
                                            start_date='2024-01-01', 
                                            end_date = datetime.now().strftime('%Y-%m-%d'),
                                            timeframe='4h'):
    session = HTTP(testnet=False, api_key=None, api_secret=None)
    all_data = []
    for symbol in symbols:
        print(f"📊 Обрабатываю {symbol}...")
        
        # kline данные (OHLCV)
        try:
            klines = session.get_kline(
                category="linear",
                symbol=symbol,
                interval=timeframe,
                start=start_date.replace('-', ''),
                end=end_date.replace('-', ''),
                limit=1000  #можно сделать пагинацию если нужно
            )
            df = pd.DataFrame(klines['result']['list'])
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                         'turnover', 'confirm', 'ignore1', 'ignore2']
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='ms')
            df['symbol'] = symbol
            df = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            df = df.astype({'open': float, 'high': float, 'low': float, 
                           'close': float, 'volume': float})
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            print(f"Загружено {len(df)} свечей")
        except Exception as e:
            print(f"Ошибка загрузки: {e}")
            continue
        
        # RSI
        df['RSI_14'] = ta.momentum.rsi(df['close'], window=14)
        df['RSI_30'] = ta.momentum.rsi(df['close'], window=30)
        
        # MACD (12,26,9)
        macd = ta.trend.MACD(df['close'], window_slow=26, window_fast=12, window_sign=9)
        df['MACD'] = macd.macd()
        df['MACD_signal'] = macd.macd_signal()
        df['MACD_histogram'] = macd.macd_diff()
        
        # VOLUME INDICATORS
        df['Volume_SMA_5'] = df['volume'].rolling(5).mean()
        df['Volume_SMA_10'] = df['volume'].rolling(10).mean()
        df['Volume_SMA_20'] = df['volume'].rolling(20).mean()
        
        df['Volume_Ratio_5'] = df['volume'] / df['Volume_SMA_5']
        df['Volume_Ratio_20'] = df['volume'] / df['Volume_SMA_20']
        
        df['Volume_Change_Pct'] = df['volume'].pct_change() * 100

        df['OBV'] = ta.volume.on_balance_volume(df['close'], df['volume'])
        
        df['Volume_MA_Ratio'] = df['volume'] / df['volume'].rolling(50).mean()
        
        df['Price_Volume'] = df['close'] * df['volume']
        
        all_data.append(df)
        print(f"Индикаторы добавлены для {symbol}")
        
        time.sleep(0.1)  
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        
        # Заполняем NaN (LOCF + forward fill)
        result = result.fillna(method='ffill').fillna(method='bfill')
        filename = f"bybit_technicals_{start_date}_to_{end_date}_{timeframe}.csv"
        result.to_csv(filename, index=False)
        print(f"\nСохранено: {filename}")
        print(f"Итого строк: {len(result)}, колонок: {len(result.columns)}")
        
        return result
    return pd.DataFrame()

# Дополнительная функция для быстрого старта
def quick_technicals(symbol='BTCUSDT'):
    """Быстрый тест на 1 месяц"""
    data = download_and_compute_technical_indicators(
        symbols=[symbol],
        start_date='2026-01-01',
        end_date='2026-02-11',
        timeframe='4h'
    )
    return data

if __name__ == "__main__":
    # Пример
    data = download_and_compute_technical_indicators(
        symbols=['BTCUSDT'], 
        timeframe='1d'
    )
    
    print("\n Пример финальных данных:")
    print(data[['timestamp', 'symbol', 'close', 'RSI_14', 'MACD', 
                'Volume_Ratio_5', 'OBV']].tail())