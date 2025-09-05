'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-08-27 20:55:11
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-05 10:12:44
FilePath: /mss_qianshou/app/main.py
Description: 

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

import os, uvicorn
from loguru import logger
from dotenv import load_dotenv

from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from qianshou.models import Equity
from qianshou.sqlite_db import *
from qianshou.hist_futu import futu_update_daily
from qianshou.account_futu import futu_sync_group

# 加载环境变量
load_dotenv()
LOG_FILE = os.getenv("LOG_FILE", "chuanyin.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "23000"))
CRON_HOUR = int(os.getenv("CRON_HOUR", "6"))
CRON_MINUTE = int(os.getenv("CRON_MINUTE", "0"))
SYNC_INTERV_M = int(os.getenv("SYNC_INTERV_M", "5"))

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
    return rows

@app.post("/equities")
def add_equity_api(e: Equity):
    e_id = add_equity(e)
    logger.info(f"添加标的, ID={e_id}")
    return {"status":"ok", "id": e_id}

@app.get("/equities/{e_id}")
def get_equity_by_id_api(e_id: int):
    row = get_equity(e_id)
    return row

@app.put("/equities/{e_id}")
def update_equity_by_id_api(e_id: int, e: Equity):
    update_equity(e_id, e)
    logger.info(f"更新标的, ID={e_id}")
    return {"status":"ok", "id": e_id}

@app.delete("/equities/{e_id}")
def delete_equity_by_id_api(e_id: int):
    delete_equity(e_id)
    logger.info(f"删除标的, ID={e_id}")
    return {"status":"ok", "id": e_id}  

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
