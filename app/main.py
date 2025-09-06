'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-08-27 20:55:11
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-06 10:33:30
FilePath: /mss_qianshou/app/main.py
Description: 

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

import os, uvicorn
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, date
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Optional
from pydantic import BaseModel, field_validator, ValidationError

from qianshou.models import Equity
from qianshou.sqlite_db import init_db, get_equities
from qianshou.indicator_tools import load_all_indicators
from qianshou.hist_futu import futu_update_daily
from qianshou.account_futu import futu_sync_group, load_equity_finance
from qianshou.bin_tools import load_equity_quote

# 加载环境变量
load_dotenv()
LOG_FILE = os.getenv("LOG_FILE", "qianshou.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "23000"))
CRON_HOUR = int(os.getenv("CRON_HOUR", "6"))
CRON_MINUTE = int(os.getenv("CRON_MINUTE", "0"))
SYNC_INTERV_M = int(os.getenv("SYNC_INTERV_M", "5"))


class DateRangeModel(BaseModel):
    start: Optional[date] = None
    end: Optional[date] = None

    @field_validator("start", mode="before")
    @classmethod
    def validate_end(cls, v):
        if v is None:
            return datetime.strptime("1990-01-01" , "%Y-%m-%d").date()
        if isinstance(v, date):
            return v
        try:
            # 强制解析固定格式"%Y-%m-%d"
            return datetime.strptime(v, "%Y-%m-%d").date()
        except Exception:
            raise ValidationError("date must be in YYYY-MM-DD format")
        
    @field_validator("end", mode="before")
    @classmethod
    def validate_start(cls, v):
        if v is None:
            return datetime.strptime("2200-01-01" , "%Y-%m-%d").date()
        if isinstance(v, date):
            return v
        try:
            # 强制解析固定格式"%Y-%m-%d"
            return datetime.strptime(v, "%Y-%m-%d").date()
        except Exception:
            raise ValidationError("date must be in YYYY-MM-DD format")
        
# 记录日志到文件，日志文件超过500MB自动轮转
logger.add(LOG_FILE, level=LOG_LEVEL, rotation="50 MB", retention=5)

# 定时任务
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    init_db()
    scheduler.add_job(futu_update_daily, "cron", 
                    day_of_week="1-5", # 每周二到周六
                    hour=CRON_HOUR, minute=CRON_MINUTE,
                    id="futu_daily")
    scheduler.add_job(futu_sync_group, "interval", 
                    minutes=SYNC_INTERV_M,
                    id="futu_sync")
    scheduler.start()
    yield
    scheduler.shutdown()
    logger.info("Shutting down...")

app = FastAPI(lifespan=lifespan, title="Qianshou Service")

@app.get("/equities")
def list_equities_api():
    rows = get_equities(only_valid=False)
    return [Equity(**row) for row in rows]

@app.get("/indicators")
def list_indicators_api():
    return load_all_indicators()

@app.post("/equity/finance")
def get_equity_finance(symbol: str, range: DateRangeModel):
    return load_equity_finance(symbol, range.start, range.end)  # type: ignore

@app.post("/equity/quote")
def get_equity_quote(symbol: str, range: DateRangeModel):
    return load_equity_quote(symbol, range.start, range.end)  # type: ignore

@app.post("/update/futu/daily")
def update_futu_daily_api():
    futu_update_daily()
    return {"status":"ok"}

@app.post("/sync/futu/group")
async def sync_futu_group_api():
    await futu_sync_group()
    return {"status":"ok"}

if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
