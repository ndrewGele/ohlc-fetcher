from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

import os

import pandas as pd
from pandas.io import sql

import random

from sqlalchemy import create_engine, delete, inspect, MetaData, Table, update 

import time

# from typing import Union

load_dotenv()
pg_db = os.environ['POSTGRES_DB']
pg_user = os.environ['POSTGRES_USER']
pg_pass = os.environ['POSTGRES_PASSWORD']
pg_host = os.environ['POSTGRES_HOST']
pg_port = os.environ['POSTGRES_PORT']

client = StockHistoricalDataClient(
  api_key = os.environ['APCA_PAPER_KEY'],
  secret_key = os.environ['APCA_PAPER_SECRET']
)

est_tz = timezone(timedelta(hours = -5))

connected = False
while not connected:
  try:
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")
    meta = MetaData()
    meta.reflect(bind = engine)
  except Exception as error:
    print('Failed to connect to database.\n'
          'Details below.\n'
          'Sleeping for 30 minutes.')
    print(error)
    print(error.__class__.__name__)
    if hasattr(error, 'traceback'): print(error.traceback)
    time.sleep(60 * 30)
    continue
  else:
    print('Database engine initialized.')
    connected = True

symbols_table_exists = False
while not symbols_table_exists:
  try:
    symbols_table = Table('symbols', meta, autoload_with = engine)
  except Exception as error:
    print('Could not find symbols table.\n'
          'Details below.\n'
          'Sleeping for 30 minutes.')
    print(error)
    print(error.__class__.__name__)
    if hasattr(error, 'traceback'): print(error.traceback)
    time.sleep(60 * 30)
    continue
  else:
    print('Symbols table found. Proceeding to main event loop.')
    symbols_table_exists = True

while True:

  print('Starting loop.')
  
  # Only look for new data at night after market closes
  if datetime.now(tz = est_tz).hour in range(6,19):
    print('Will only fetch data from 7pm until 6am.\n'
          'Sleeping for 30 minutes.')
    time.sleep(60 * 30)
    continue
  
  print('Valid fetch hour. Starting symbol selection.')

  with engine.connect() as connection:

    with open('./sql/symbols_list.sql') as query:
      symbol_df =  pd.read_sql(
        sql = query.read(),
        con = connection
      )
      symbol_list = symbol_df['symbol']

    print('Symbol data retrieved.')
  
    # Pick a random symbol if table doesn't exist yet
    try:
      daily_ohlc_table = Table('daily_ohlc', meta, autoload_with = engine)
    except Exception as error:
      picked = random.choice(symbol_list)
      print(f'Daily OHLC Table does not exist yet.\n'
            f'Picked {picked} randomly from missing symbols.')
      
    # If table does exist, we'll use the data within to pick a symbol
    else:
      print('Daily OHLC Table found.')
      with open('./sql/ohlc_stats.sql') as query:
        ohlc_stats =  pd.read_sql(
          sql = query.read().format(
            symbols = "('" + "', '".join(symbol_list) + "')"
          ),
          con = connection
        )

      print(ohlc_stats)

      # End of engine.connect()
    
      if any([symbol not in ohlc_stats['symbol'] for symbol in symbol_list]):
        print('missing')
        missing = [symbol for symbol in symbol_list if symbol not in ohlc_stats['symbol']]
        print(missing)
        picked = random.choice(missing)
        print(f'Not all symbols present in table.\n'
              f'Picked {picked} randomly from missing symbols.')
      
      else:
        print('oldest')
        oldest_df = ohlc_stats[ohlc_stats['max_update'] == min(ohlc_stats['max_update'])]
        picked = random.choice(oldest_df['symbol'])
        print(f'All symbols present in table.\n'
              f'Picked {picked} randomly from oldest symbols.')   

  # This is the end of the symbol picker.
  
  request_params = StockBarsRequest(
    symbol_or_symbols = picked.upper(),
    timeframe = TimeFrame.Day,
    start = datetime.today().replace(hour = 0, minute = 0) - timedelta(days = 3000),
    end = datetime.today().replace(hour = 23, minute = 59) - timedelta(days = 1),
    limit = 10000
  )
    
  try:
    print('Trying to get stock bars...')
    new_bars = client.get_stock_bars(request_params)
  
  except AttributeError as error:
    if 'has no attribute\'items\'' in str(error):
      print(f'Attribute error encountered for {picked}.\n'
            f'This probably means the Alpaca Market API has no data for {picked}.'
            f'Will try to remove {picked} from the Symbols table.')
      with engine.connect() as connection:
        connection.execute(
            delete(symbols_table)
              .where(symbols_table.c.symbol == picked)
          )
      

  except Exception as error:
    print('Exception encountered!')
    print(error)
    print(error.__class__.__name__)
    if hasattr(error, 'traceback'): print(error.traceback)  
    time.sleep(60 * 10)
    continue

  else:
    print('API request successful!')
    new_bars_df = new_bars.df
    new_bars_df['update_timestamp'] = [datetime.now(est_tz).replace(microsecond= 0 )] * len(new_bars_df)
    new_bars_df.reset_index(inplace=True)
    
    with engine.connect() as connection:
      
      # If table already exists, we'll do an anti-join before appending table
      if inspect(engine).has_table('daily_ohlc'):
        existing = pd.read_sql(
          sql = f'select * from daily_ohlc where symbol = \'{picked.upper()}\'',
          con = connection
        )
        
        new_bars_df = pd.merge(
          left = new_bars_df, 
          right = existing[['symbol', 'timestamp']], 
          on = ['symbol', 'timestamp'], 
          how = 'left', 
          indicator = 'exists'
        )
        new_bars_df = new_bars_df[new_bars_df['exists'] == 'left_only']
        new_bars_df.drop(['exists'], axis = 1, inplace = True)
      
      if len(new_bars_df) > 0:
        new_bars_df.to_sql(
          name = 'daily_ohlc',
          con = connection,
          if_exists = 'append',
          index = False
        )
      
      if len(new_bars_df) > 0:
        if 'daily_ohlc_table' not in locals():
          daily_ohlc_table = Table('daily_ohlc', meta, autoload_with = engine)
        connection.execute(
          update(daily_ohlc_table)
            .values({'update_timestamp': datetime.now(est_tz).replace(microsecond= 0 )})
            .where(daily_ohlc_table.c.symbol == picked)
        )
  
  # Log and sleep if everything was successful.
  # Log API response if edge case.
  print(f'Added {len(new_bars_df)} rows of data for {picked}.')
  if len(new_bars_df) == 0:
    print('Sleeping for ten minutess since no new data was found.')
    time.sleep(60 * 10)
  else:
    print('Sleeping for 15 seconds before running again.')
    time.sleep(15)
  
  # This is the end of the while loop.
