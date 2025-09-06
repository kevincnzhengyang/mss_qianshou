'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-09-04 19:05:51
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-06 16:20:26
FilePath: /mss_qianshou/app/qianshou/account_futu.py
Description: 

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''


import os, json, asyncio, time
import pandas as pd
from datetime import date
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
import akshare as ak
from futu import OpenQuoteContext, RET_OK

from .models import Equity
from .sqlite_db import *

# 加载环境变量
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".." / ".env")
FUTU_API_HOST = os.getenv("FUTU_API_HOST", "127.0.0.1")
FUTU_API_PORT = int(os.getenv("FUTU_API_PORT", "21111"))
FUTU_GROUP_NAME = os.getenv("FUTU_GROUP_NAME", "量化分析")
DATA_DIR = os.path.expanduser(os.getenv("DATA_DIR", "~/Quanter/qlib_data"))
RPT_DIR = os.path.join(DATA_DIR, "finance")   # 年度财务报表

# 初始化各个子路径和文件
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RPT_DIR, exist_ok=True)


def _create_and_doc(symbol: str, market: str) -> None:
    # 创建标的，并利用AKShare获取基本信息（因Futu9.4不提供此类接口）
    e = Equity(symbol=symbol, market=market)
    if market == 'HK':
        info = ak.stock_individual_basic_info_hk_xq(symbol=symbol)
    elif market == 'SH' or market == 'SZ':
        info = ak.stock_individual_basic_info_xq(symbol=f"{market}{symbol}")
    else:
        info = None
    if info is None or not isinstance(info, pd.DataFrame) or info.empty:
        e.note = ""
    else:
        e.note = json.dumps(info.to_dict(orient="records"))
    add_equity(e)
    logger.info(f"创建标的 {e.symbol}@{e.market} 成功")
    time.sleep(5)

def _format_report(df: pd.DataFrame, market: str) -> pd.DataFrame:
    if df.empty:
        return df
    
    df['date'] = pd.to_datetime(df['REPORT_DATE'])
    df['date'] = df['date'].dt.date

    # 去掉无用的列
    df.drop(columns=['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'FISCAL_YEAR',
                     'ORG_CODE', 'ORG_TYPE', 'REPORT_TYPE', 'REPORT_DATE', 'REPORT_DATE_NAME',
                     'SECURITY_TYPE_CODE', 'NOTICE_DATE', 'START_DATE', 
                     'UPDATE_DATE', 'DATE_TYPE_CODE', 'STD_REPORT_DATE'
                     ], errors='ignore', inplace=True)
    
    if market == "HK":
        # 用pivot将STD_ITEM_NAME作为列，AMOUNT作为值，按date聚合
        df = df.pivot(index='date', columns='STD_ITEM_NAME', values='AMOUNT').reset_index()
    
    return df.set_index("date").reset_index()

def _request_balance(symbol: str, market: str) -> None:
    csv_file = os.path.join(RPT_DIR, f"balance_{symbol}.csv")
    if os.path.exists(csv_file):
        logger.info(f"资产负债表已经存在: {symbol}@{market}=>{csv_file}")
        return
    
    df = None
    for i in range(3):
        try:
            if market == 'HK':
                df = ak.stock_financial_hk_report_em(stock=symbol, symbol="资产负债表", indicator="年度")
            elif market == 'SH' or market == 'SZ':
                df = ak.stock_balance_sheet_by_yearly_em(symbol=f"{market}{symbol}")
            break
        except Exception as e:
            logger.warning(f"获取资产负债表失败: {symbol}@{market} --> {i}")
            continue
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning(f"获取资产负债表失败: {symbol}@{market}")
    else:
        df = _format_report(df, market)
        df.to_csv(csv_file, index=False)
        logger.info(f"获取资产负债表成功: {symbol}@{market}") 


def _request_profit(symbol: str, market: str) -> None:
    csv_file = os.path.join(RPT_DIR, f"profit_{symbol}.csv")
    if os.path.exists(csv_file):
        logger.info(f"利润表已经存在: {symbol}@{market}=>{csv_file}")
        return
    
    df = None
    for i in range(3):
        try:
            if market == 'HK':
                df = ak.stock_financial_hk_report_em(stock=symbol, symbol="利润表", indicator="年度")
            elif market == 'SH' or market == 'SZ':
                df = ak.stock_profit_sheet_by_yearly_em(symbol=f"{market}{symbol}")
            break
        except Exception as e:
            logger.warning(f"获取利润表失败: {symbol}@{market} --> {i}")
            continue
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning(f"获取资利润表失败: {symbol}@{market}")
    else:
        df = _format_report(df, market)
        df.to_csv(csv_file, index=False)
        logger.info(f"获取利润表表成功: {symbol}@{market}") 


def _request_cashflow(symbol: str, market: str) -> None:
    csv_file = os.path.join(RPT_DIR, f"cashflow_{symbol}.csv")
    if os.path.exists(csv_file):
        logger.info(f"现金流量表已经存在: {symbol}@{market}=>{csv_file}")
        return
    
    df = None
    for i in range(3):
        try:
            if market == 'HK':
                df = ak.stock_financial_hk_report_em(stock=symbol, symbol="现金流量表", indicator="年度")
            elif market == 'SH' or market == 'SZ':
                df = ak.stock_cash_flow_sheet_by_yearly_em(symbol=f"{market}{symbol}")
            break
        except Exception as e:
            logger.warning(f"获取现金流量表失败: {symbol}@{market} --> {i}")
            continue
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning(f"获取现金流量表失败: {symbol}@{market}")
    else:
        df = _format_report(df, market)
        df.to_csv(csv_file, index=False)
        logger.info(f"获取现金流量表成功: {symbol}@{market}") 

def request_hist_finance(f_list: list) -> None:
    if len(f_list) == 0:
        return
    for (symbol, market) in f_list:
        logger.debug(f"开始下载财务数据{symbol}@{market}...")
        _request_balance(symbol, market)
        _request_profit(symbol, market)
        _request_cashflow(symbol, market)
        logger.debug(f"完成下载财务数据{symbol}@{market}!")
        time.sleep(5)

async def futu_sync_group():
    logger.debug(f"开始同步富途牛牛自选股列表...")
    quote_ctx = OpenQuoteContext(host=FUTU_API_HOST, port=FUTU_API_PORT)

    f_list = []
    equities = []
    ret, data = quote_ctx.get_user_security(FUTU_GROUP_NAME)
    if ret != RET_OK or data is None or not isinstance(data, pd.DataFrame) or data.empty:
        logger.warning(f"富途牛牛中获取自选列表{FUTU_GROUP_NAME}失败: {data}")
    elif data.shape[0] > 0:  # 如果自选股列表不为空
        equities = data['code'].values.tolist()
        logger.info(f"自选列表: {equities}")   # 转为 list
    # 关闭Futu
    quote_ctx.close()

    # 同步数据
    for code in equities:
        market, symbol = code.split(".")
        market = market.upper()
        symbol = symbol.upper()
        logger.info(f"同步{symbol}@{market}")
        f_list.append((symbol, market))
        if if_not_exist_equity(symbol):
            _create_and_doc(symbol, market)

    # 利用AKShare下载历史财报数据（因Futu9.4不提供此类接口）
    if f_list:
        await asyncio.to_thread(request_hist_finance, f_list)
    
    # 清理已经不在列表中
    # clear_others_equities(equities)
    logger.debug(f"完成同步富途牛牛自选股列表!")

def load_equity_finance(symbol: str, start_date: date, end_date: date) -> dict:
    res = dict()

    row = get_equity_by_symbol(symbol=symbol)
    if row is None:
        logger.error(f"找不到股票{symbol}，无法获得财务数据")
        return res
    
    e = Equity(**row)
    logger.debug(f"找到股票{e.symbol}")


    # 资产负债表
    csv_file = os.path.join(RPT_DIR, f"balance_{symbol}.csv")
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        df = df.reset_index()
        df['date'] = df['date'].dt.date
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        df = df.replace({float('nan'): None})
        res["BalanceSheet"] = df.to_dict(orient="records")
    else:
        res["BalanceSheet"] = []

    # 利润表
    csv_file = os.path.join(RPT_DIR, f"profit_{symbol}.csv")
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        df = df.reset_index()
        df['date'] = df['date'].dt.date
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        df = df.replace({float('nan'): None})
        res["ProfitSheet"] = df.to_dict(orient="records")
    else:
        res["ProfitSheet"] = []

    # 现金流表
    csv_file = os.path.join(RPT_DIR, f"cashflow_{symbol}.csv")
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        df = df.reset_index()
        df['date'] = df['date'].dt.date
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        df = df.replace({float('nan'): None})
        res["CashFlow"] = df.to_dict(orient="records")
    else:
        res["CashFlow"] = []
    return res
