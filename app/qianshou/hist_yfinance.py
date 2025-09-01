'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-08-27 22:49:51
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-01 21:04:30
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
from .indicator_tools import IndicatorManager
from .bin_tools import *


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

def _update_equity(e: Equity, manager: IndicatorManager):
    yf_name = e.to_yfinance_symbol()
    ft_name = e.to_futu_symbol()
    logger.debug(f"准备更新标的{yf_name}==={ft_name}")
    
    ocsv_file = os.path.join(OCSV_DIR, f"{ft_name}.csv")
    logger.info(f"原始数据文件: {ocsv_file}")

    # 读取已有csv
    if os.path.exists(ocsv_file):
        df = pd.read_csv(ocsv_file, index_col=0, parse_dates=True)
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
            logger.info(f"没有历史行情数据 {yf_name} from {start_date} to {today}")
            return
        
        # 重新整理格式
        new_data = _format_dataframe(new_data, ft_name)

        # 合并数据
        df = pd.concat([df, new_data])

        # 保存原始的CSV
        df.to_csv(ocsv_file)

        # 计算各种指标
        df_with_ind = manager.calculate(df) 

        # 保存有指标结果的CSV
        csv_file = os.path.join(CSV_DIR, f"{ft_name}.csv")
        df_with_ind.to_csv(csv_file)
        logger.info(f"更新数据文件: {csv_file}, 总记录数: {len(new_data)} => {yf_name} {start_date} - {today}")
        logger.info(f"待分析数据文件: {csv_file}")
    else:
        logger.warning(f"尝试下载行情数据失败: {yf_name}: {start_date} - {today}")

def yfinance_update_daily():
    # 加载指标管理
    manager = IndicatorManager()
    manager.load_all_sets()

    for row in get_equities(only_valid=True):
        e = Equity(**dict(row))
        _update_equity(e, manager)
    
    # 转换为Qlib的BIN格式
    convert_csv_to_bin()
    
    # 更新最后更新时间
    set_equities_last()
