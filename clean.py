import akshare as ak
import pandas as pd
from datetime import datetime
import pytz
import pyarrow
import duckdb

tz = pytz.timezone('Asia/Shanghai')
today = datetime.now(tz).strftime('%Y%m%d')


tradeday = ak.tool_trade_date_hist_sina()
tradeday['trade_date'] = pd.to_datetime(tradeday['trade_date'])

if sum(tradeday['trade_date'] == today):
    df1 = pd.read_parquet(f'data/raw/price_{today}.parquet')
    df1['日期'] = pd.to_datetime(today)
    df1 = df1[['日期','代码','今开','最新价','最高','最低','成交量','成交额','振幅','涨跌幅','涨跌额','换手率','量比','市盈率-动态','市净率','总市值','流通市值']].copy()
    df1.rename(columns={'代码':'股票代码','今开':'开盘','最新价':'收盘'},inplace=True)

    if [i for i in os.listdir('data/clean') if i.endswith('parquet')]:
        df2 = duckdb.read_parquet('data/clean/*.parquet')
        con_df = pd.concat([df1,df2],ignore_index=True)
    else:
        con_df = df1

    con_df.drop_duplicates(inplace=True)
    con_df.to_parquet(f'data/clean/price_{today}.parquet',engine='pyarrow',index=False)
