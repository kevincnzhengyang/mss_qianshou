'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-09-01 10:23:51
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-01 20:47:12
FilePath: /mss_qianshou/app/qianshou/bin_tools.py
Description: QLib BIN文件工具

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

import os, sys, shutil
import pandas as pd
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
from qlib.data import D
from qlib.data.storage import FeatureStorage


# 加载环境变量
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".." / ".env")
DATA_DIR = os.path.expanduser(os.getenv("DATA_DIR", "~/Quanter/qlib_data"))
QLIB_DIR = os.path.expanduser(os.getenv("QLIB_DIR", "~/Quanter/qlib"))
OCSV_DIR = os.path.join(DATA_DIR, "ocsv")   # for original csv data
CSV_DIR = os.path.join(DATA_DIR, "csv")     # for csv data with all indicators

# 初始化各个子路径和文件
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OCSV_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "calendars"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "features"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "instruments"), exist_ok=True)

def convert_csv_to_bin() -> None:
    # 准备导出BIN格式的工具脚本
    sys.path.append(os.path.join(QLIB_DIR, "scripts"))
    from dump_bin import DumpDataAll  # type: ignore warnings

    # 转换为Qlib的BIN格式
    dump = DumpDataAll(
        data_path=CSV_DIR,
        qlib_dir=DATA_DIR,
        exclude_fields="date,symbol")
    dump.dump()
    logger.info(f"将数据由CSV转换为BIN")
