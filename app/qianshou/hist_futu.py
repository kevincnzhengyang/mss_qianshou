'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-09-01 21:06:32
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-05 09:58:56
FilePath: /mss_qianshou/app/qianshou/hist_futu.py
Description: 

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

import os, requests
import pandas as pd
import akshare as ak
import time as t
from loguru import logger
from datetime import datetime, timedelta, time
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


def _get_public_ip() -> str:
    ip = requests.get("https://api.ipify.org").text
    return ip

def _get_geo_info(ip) -> tuple:
    url = f"http://ip-api.com/json/{ip}?lang=zh-CN"
    resp = requests.get(url)
    data = resp.json()
    if data['status'] == 'success':
        return data['country'], data['regionName']
    else:
        return None, None

def _is_chinese_mainland() -> bool:
    ip = _get_public_ip()
    country, region = _get_geo_info(ip)
    if country != "中国":
        return False
    if region != "香港" or region != "澳门" or region != "台湾":
        return False
    return True
 
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
        start_date = (last_date + timedelta(days=1)).date()
    else:
        df = pd.DataFrame()
        start_date = datetime.strptime("1990-01-01", "%Y-%m-%d").date()

    # 下载增量数据
    if datetime.now().time() < time(17, 0, 0):
        today = (datetime.today() - timedelta(days=1)).date()
    else:
        today = datetime.today().date()
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
                fields=[KL_FIELD.DATE_TIME, 
                        KL_FIELD.OPEN, KL_FIELD.HIGH, 
                        KL_FIELD.LOW, KL_FIELD.CLOSE, 
                        KL_FIELD.TRADE_VOL, 
                        KL_FIELD.TRADE_VAL,         # 成交额
                        KL_FIELD.PE_RATIO,          # 市盈率
                        KL_FIELD.TURNOVER_RATE],    # 换手率
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
    t.sleep(3)

def _ak_request_history(symbol: str, start: str, end: str) -> pd.DataFrame | None:  
    logger.debug(f"AK获取历史数据{symbol} {start}-{end}")  
    df = None
    for i in range(3):
        try:
            df = ak.stock_zh_a_hist(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
            break
        except Exception as e:
            logger.warning(f"AK获取历史数据失败: {symbol} {start}-{end} --> {i}")
            continue
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    
    # 整理格式
    # 改名
    df = df.rename(columns={
                "日期": "date", 
                "股票代码": "symbol", 
                "开盘": "open", 
                "收盘": "close", 
                "最高": "high", 
                "最低": "low", 
                "成交量": "volume", 
                "成交额": "turnover", 
                "换手率": "turnover_rate"})

    # 去掉"振幅", "涨跌幅", "涨跌额
    df = df.drop(columns=["振幅", "涨跌幅", "涨跌额"], errors="ignore")

    # 转换为 datetime
    df["date"] = pd.to_datetime(df["date"])

    # 为市盈率赋值
    df["pe_ratio"] = 0.0


    # 设置为索引
    return df.set_index("date")

def _akshare_update_equity(e: Equity, manager: IndicatorManager):
    ft_name = e.to_futu_symbol()
    ak_name = e.to_akshare_name()
    logger.debug(f"AK准备更新标的 {ak_name}")
    
    ocsv_file = os.path.join(OCSV_DIR, f"{ft_name}.csv")
    logger.info(f"原始数据文件: {ocsv_file}")

    # 读取已有csv
    if os.path.exists(ocsv_file):
        df = pd.read_csv(ocsv_file, index_col=0, parse_dates=True)
        last_date = df.index.max()
        start_date = (last_date + timedelta(days=1)).date()
    else:
        df = pd.DataFrame()
        start_date = datetime.strptime("1990-01-01", "%Y-%m-%d").date()

    # 下载增量数据
    if datetime.now().time() < time(17, 0, 0):
        today = (datetime.today() - timedelta(days=1)).date()
    else:
        today = datetime.today().date()
    
    logger.info(f"{ft_name}: {start_date} - {today}")

    if start_date <= today:
        # 获取行情
        data = _ak_request_history(symbol=ak_name, start=start_date.strftime("%Y%m%d"), end=today.strftime("%Y%m%d"))
        if data is None or not isinstance(data, pd.DataFrame) or data.empty:
            logger.info(f"AK没有历史行情数据 {ak_name} from {start_date} to {today}")
        else:
            # 合并数据
            df = pd.concat([df, data])

            # 保存原始的CSV
            df.to_csv(ocsv_file)
            logger.info(f"AK 更新数据文件: {ocsv_file}, 总记录数: {len(df)} => {ak_name} {start_date} - {today}")
        
    else:
        logger.warning(f"AK尝试下载行情数据失败: {ak_name}: {start_date} - {today}")  


    # 计算各种指标，即使数据无更新，自定义指标库也可能已发生变化，重新计算
    df_with_ind = manager.calculate(df) 

    # 保存有指标结果的CSV
    csv_file = os.path.join(CSV_DIR, f"{ft_name}.csv")
    df_with_ind.to_csv(csv_file)
    logger.info(f"待分析数据文件: {csv_file}")
    t.sleep(3)

def futu_update_daily():
    a_shares = []

    # 连接 FUTU
    quote_ctx = OpenQuoteContext(host=FUTU_API_HOST, port=FUTU_API_PORT)

    # 加载指标管理
    manager = IndicatorManager()
    manager.load_all_sets()

    for row in get_equities(only_valid=True):
        e = Equity(**dict(row))
        _update_equity(e, manager, quote_ctx)
        if e.market == 'SH' or e.market == 'SZ':
            a_shares.append(e)
    
    quote_ctx.close()

    # 摘自FUTU API 文档：
    # - 中国内地 IP 个人客户：免费获取 LV1 行情
    # - 港澳台及海外IP客户/机构客户：暂不支持
    # 
    # 当位置不在大陆时，使用akshre获取历史数据
    if not _is_chinese_mainland():
        for e in a_shares:
            _akshare_update_equity(e, manager)

    # 转换为Qlib的BIN格式
    convert_csv_to_bin()
    
    # 更新最后更新时间
    set_equities_last()

