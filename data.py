import akshare as ak
import pandas as pd
from datetime import datetime
import pytz
import pyarrow

tz = pytz.timezone('Asia/Shanghai')
today = datetime.now(tz).strftime('%Y%m%d')

tradeday = ak.tool_trade_date_hist_sina()
tradeday['trade_date'] = pd.to_datetime(tradeday['trade_date'])

if sum(tradeday['trade_date'] == today):  
    df = ak.stock_zh_a_spot_em()
    df.to_parquet(f'data/raw/price_{today}.parquet', engine='pyarrow', index=False)
