'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-08-27 22:49:51
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-08-28 22:10:37
FilePath: /mss_qianshou/app/qianshou/hist_yfinance.py
Description: 使用yfiannce 获取历史数据

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

import os, sys
import yfinance as yf
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

from .models import Equity
from .sqlite_db import get_equities, set_equities_last


# 加载环境变量
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".." / ".env")
DATA_DIR = os.path.expanduser(os.getenv("DATA_DIR", "~/.qlib/qlib_data"))
QLIB_PATH = os.path.expanduser(os.getenv("QLIB_PATH", "~/Quanter/qlib"))
CN_BIN_DIR = os.path.join(DATA_DIR, "cn_data")
CN_CSV_DIR = os.path.join(CN_BIN_DIR, "csv")
US_BIN_DIR = os.path.join(DATA_DIR, "us_data")
US_CSV_DIR = os.path.join(US_BIN_DIR, "csv")
HK_BIN_DIR = os.path.join(DATA_DIR, "hk_data")
HK_CSV_DIR = os.path.join(HK_BIN_DIR, "csv")
TW_BIN_DIR = os.path.join(DATA_DIR, "tw_data")
TW_CSV_DIR = os.path.join(TW_BIN_DIR, "csv")
JP_BIN_DIR = os.path.join(DATA_DIR, "jp_data")
JP_CSV_DIR = os.path.join(JP_BIN_DIR, "csv")
UK_BIN_DIR = os.path.join(DATA_DIR, "uk_data")
UK_CSV_DIR = os.path.join(UK_BIN_DIR, "csv")
os.makedirs(CN_BIN_DIR, exist_ok=True)
os.makedirs(CN_CSV_DIR, exist_ok=True)
os.makedirs(US_BIN_DIR, exist_ok=True)
os.makedirs(US_CSV_DIR, exist_ok=True)
os.makedirs(HK_BIN_DIR, exist_ok=True)
os.makedirs(HK_CSV_DIR, exist_ok=True)
os.makedirs(TW_BIN_DIR, exist_ok=True)
os.makedirs(TW_CSV_DIR, exist_ok=True)
os.makedirs(JP_BIN_DIR, exist_ok=True)
os.makedirs(JP_CSV_DIR, exist_ok=True)
os.makedirs(UK_BIN_DIR, exist_ok=True)
os.makedirs(UK_CSV_DIR, exist_ok=True)

_MARKET_DIR = {
    'US': (US_CSV_DIR, US_BIN_DIR),
    'HK': (HK_CSV_DIR, HK_BIN_DIR),
    'CN': (CN_CSV_DIR, CN_BIN_DIR),
    'TW': (TW_CSV_DIR, TW_BIN_DIR),
    'JP': (JP_CSV_DIR, JP_BIN_DIR),
    'UK': (UK_CSV_DIR, UK_BIN_DIR)
}

# 准备导出BIN格式的工具脚本
sys.path.append(os.path.join(QLIB_PATH, "scripts"))
from dump_bin import DumpDataUpdate


def _format_dataframe(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    # 如果返回 MultiIndex（某些版本可能出现），统一处理：
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    # 增加 symbol 列
    df["symbol"] = symbol
    
    # 将索引日期转成列
    df = df.reset_index()

    # 按 QLib 要求重命名列
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    # 只保留需要的列
    df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]

    # 将date作为index准备与已有数据合并
    return df.set_index("date")

def _update_equity(e: Equity, markets: set):
    yf_name = e.to_yfinance_symbol()
    ft_name = e.to_futu_symbol()
    csv_file = f"{ft_name}.csv"
    logger.debug(f"准备更新标的{yf_name}==={ft_name}")

    match e.market:
        case 'US':
            markets.add('US')
        case 'HK':
            markets.add('HK')
        case 'SH' | 'SZ':
            markets.add('CN')
        case 'TW':
            markets.add('TW')
        case 'JP':
            markets.add('JP')
        case 'UK':
            markets.add('UK')
        case _:
            raise ValueError(f"不支持的市场: {e.market}")
    
    # 读取已有csv
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        last_date = df.index.max()
        start_date = last_date + timedelta(days=1)
    else:
        df = pd.DataFrame()
        start_date = datetime.strptime("1990-01-01", "%Y-%m-%d")

    # 下载增量数据
    today = datetime.today()
    logger.info(f"{yf_name}: {start_date} - {today}")
    if start_date <= today:
        new_data = yf.download(yf_name, 
                               start=start_date.strftime("%Y-%m-%d"), 
                               end=today.strftime("%Y-%m-%d"), 
                               interval="1d")
        if new_data is None or new_data.empty:
            logger.info(f"Empty quote for {yf_name} from {start_date} to {today}")
            return
        
        # 重新整理格式
        new_data = _format_dataframe(new_data, ft_name)

        # 合并数据
        df = pd.concat([df, new_data])

        # 保存CSV
        df.to_csv(csv_file)
        logger.info(f"Update {len(new_data)} quote for {yf_name} from {start_date} to {today}")
    else:
        logger.warning(f"try to download {yf_name}: {start_date} > {today}")

def yfinance_update_daily():
    markets = set()

    for row in get_equities(only_valid=True):
        e = Equity(**dict(row))
        _update_equity(e, markets)
    
    # 转换为Qlib的BIN格式
    for mkt in list(markets):
        dump = DumpDataUpdate(
            data_path=_MARKET_DIR[mkt][0],
            qlib_dir=_MARKET_DIR[mkt][1],
            exclude_fields="date,symbol")
        dump.dump()
        logger.info(f"将{mkt}数据由CSV转换为BIN")
    set_equities_last()

