'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-08-27 20:58:59
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-06 16:03:40
FilePath: /mss_qianshou/app/qianshou/sqlite_db.py
Description: 

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

import os, sqlite3
from pathlib import Path
from typing import Any
from loguru import logger
from dotenv import load_dotenv

from .models import Equity


# 加载环境变量
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".." / ".env")
DB_FILE = os.getenv("DB_FILE", "qianshou.db")


# 初始化数据库
def init_db():
    # 确保数据库文件存在    
    if not os.path.exists(DB_FILE):
        logger.info(f"数据库文件不存在，创建数据库文件：{DB_FILE}")
        with open(DB_FILE, "w") as f:
            f.write("") 
        logger.info(f"数据库文件创建成功：{DB_FILE}")
    else:
        logger.info(f"数据库文件已存在：{DB_FILE}") 
    
    # 连接数据库
    conn = sqlite3.connect(DB_FILE)
    # 创建游标
    cur = conn.cursor()

    # 创建表
    cur.execute("""CREATE TABLE IF NOT EXISTS equities(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL UNIQUE, market TEXT NOT NULL, 
        note TEXT, enabled INTEGER DEFAULT 1, 
        last_date TIMESTAMP, updated_at TIMESTAMP
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_equities_symbol ON equities(symbol)")
    # 提交事务并关闭连接
    conn.commit()
    conn.close()
    logger.info("数据库初始化完成")

def add_equity(e: Equity) -> Any:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT INTO equities(symbol,market,note,enabled,updated_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP)",
                (e.symbol.upper(), e.market.upper(), e.note, int(e.enabled)))
    conn.commit()
    rule_id = cur.lastrowid
    conn.close()
    return rule_id

def get_equities(only_valid: bool = True) -> list[Any]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    if only_valid:
        rows = conn.execute("SELECT * FROM equities WHERE enabled=1").fetchall()
    else:
        rows = conn.execute("SELECT * FROM equities").fetchall()
    conn.close()
    return rows

def get_equity(e_id: int) -> Any:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM equities WHERE id=?", (e_id,)).fetchone()
    conn.close()
    return row

def get_equity_by_symbol(symbol: str) -> Any:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM equities WHERE symbol=?", (symbol.upper(),)).fetchone()
    conn.close()
    return row

def if_not_exist_equity(symbol: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM equities WHERE symbol=?", (symbol.upper(),)).fetchone()
    conn.close()
    return (row is None)

def update_equity(e_id: int, e: Equity) -> Any:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("UPDATE equities SET symbol=?,market=?,note=?,enabled=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                 (e.symbol.upper(), e.market.upper(), e.note, int(e.enabled), e_id))
    conn.commit()
    conn.close()
    return e_id

def set_equities_last() -> Any:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("UPDATE equities SET last_date=CURRENT_TIMESTAMP WHERE enabled=1")
    conn.commit()
    conn.close()

def delete_equity(rule_id: int) -> None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("UPDATE equities SET enabled=0 WHERE id=?", (rule_id,))
    conn.commit()
    conn.close()

def purge_equity(rule_id: int) -> None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("DELETE FROM equities WHERE id=?", (rule_id,))
    conn.commit()
    conn.close()

def clear_others_equities(l: list) -> None:
    if not isinstance(l, list) or len(l) == 0:
        return
    ph = ','.join('?' for _ in l)
    conn = sqlite3.connect(DB_FILE)
    conn.execute(f"DELETE FROM equities WHERE symbol NOT IN ({ph})", l)
    logger.debug(f"clear sql = DELETE FROM equities WHERE symbol NOT IN ({ph}) {l}")
    conn.commit()
    conn.close()
