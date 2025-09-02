'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-09-01 21:06:32
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-02 20:06:33
FilePath: /mss_qianshou/app/qianshou/hist_futu.py
Description: 

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

import os, sys
import yfinance as yf
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from futu import OpenQuoteContext, RET_OK, KL_FIELD

from .models import Equity
from .sqlite_db import get_equities, set_equities_last
from .indicator_tools import IndicatorManager
from .bin_tools import *



# 加载环境变量
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".." / ".env")
FUTU_API_HOST = os.getenv("FUTU_API_HOST", "127.0.0.1")
FUTU_API_PORT = int(os.getenv("FUTU_API_PORT", "21111"))


def _format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    # 改名
    df = df.rename(columns={"time_key": "date", "code": "symbol"})

    # 去掉name
    df = df.drop(columns=["name"], errors="ignore")

    # 转换为 datetime
    df["date"] = pd.to_datetime(df["date"])


    # 设置为索引
    return df.set_index("date")

def _update_equity(e: Equity, manager: IndicatorManager, ctx: OpenQuoteContext):
    ft_name = e.to_futu_symbol()
    logger.debug(f"准备更新标的 {ft_name}")
    
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
    logger.info(f"{ft_name}: {start_date} - {today}")

    if start_date <= today:
        # 分页获取行情
        all_data = []
        all_data.append(df)
        last_end = None
        while True:
            ret, data, last_page = ctx.request_history_kline(
                code=ft_name,
                start=start_date.strftime("%Y-%m-%d"),
                end=today.strftime("%Y-%m-%d"),
                ktype="K_DAY",
                max_count=1000,
                page_req_key=last_end,
                fields=[KL_FIELD.DATE_TIME, KL_FIELD.OPEN, KL_FIELD.HIGH, KL_FIELD.LOW, KL_FIELD.CLOSE, KL_FIELD.TRADE_VOL],
            )
            if ret != RET_OK or data is None or not isinstance(data, pd.DataFrame) or data.empty:
                logger.info(f"没有历史行情数据 {ft_name} from {start_date} to {today}: {ret} {data}")
                break
            
            # 重新整理格式，并保存到列表中
            all_data.append(_format_dataframe(data))

            if last_page is None:  # 没有更多分页
                logger.info(f"最后一页行情数据{ft_name} from {start_date} to {today}")
                break
            last_end = last_page
            logger.info(f"中间页行情数据{ft_name} from {start_date} to {today}")
        
        if len(all_data) == 1:
            logger.info(f"没有历史行情数据 {ft_name} from {start_date} to {today}")
        else:
            # 合并数据
            df = pd.concat(all_data)
            print(f"==== dataframe: {df.tail(5)}")

            # 保存原始的CSV
            df.to_csv(ocsv_file)
            logger.info(f"更新数据文件: {ocsv_file}, 总记录数: {len(df)} => {ft_name} {start_date} - {today}")
        
    else:
        logger.warning(f"尝试下载行情数据失败: {ft_name}: {start_date} - {today}")
    

    # 计算各种指标，即使数据无更新，自定义指标库也可能已发生变化，重新计算
    df_with_ind = manager.calculate(df) 

    # 保存有指标结果的CSV
    csv_file = os.path.join(CSV_DIR, f"{ft_name}.csv")
    df_with_ind.to_csv(csv_file)
    logger.info(f"待分析数据文件: {csv_file}")

def futu_update_daily():
    # 连接 FUTU
    quote_ctx = OpenQuoteContext(host=FUTU_API_HOST, port=FUTU_API_PORT)

    # 加载指标管理
    manager = IndicatorManager()
    manager.load_all_sets()

    for row in get_equities(only_valid=True):
        e = Equity(**dict(row))
        _update_equity(e, manager, quote_ctx)
    
    quote_ctx.close()

    # 转换为Qlib的BIN格式
    convert_csv_to_bin()
    
    # 更新最后更新时间
    set_equities_last()

