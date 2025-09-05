'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-09-01 09:13:04
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-05 18:41:07
FilePath: /mss_qianshou/app/qianshou/indicator_tools.py
Description: 

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''
import pandas as pd
import numpy as np
import os, json, re
import talib
from pathlib import Path
from typing import Dict, List
from loguru import logger
from dotenv import load_dotenv

from .models import IndicatorSet

# 加载环境变量
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".." / ".env")
INDS_DIR = os.path.join(BASE_DIR, ".." , os.getenv("INDS_DIR", "indicators"))


class IndicatorEngine:
    def __init__(self):
        self.sets: Dict[str, Dict[str, str]] = {}
        self.context_base = {
            # 基础行情
            "OPEN": None, "HIGH": None, "LOW": None, "CLOSE": None, "VOL": None,

            # 常用函数
            "REF": lambda x, n: x.shift(n),
            "MA": lambda x, n: x.rolling(n).mean(),
            "STD": lambda x, n: x.rolling(n).std(),
            "MAX": lambda x, n: x.rolling(n).max(),
            "MIN": lambda x, n: x.rolling(n).min(),

            # 均线类
            "SMA": lambda x, n: talib.SMA(x, n),
            "EMA": lambda x, n: talib.EMA(x, n),
            "WMA": lambda x, n: talib.WMA(x, n),
            "KAMA": lambda x, n: talib.KAMA(x, n),      # Kaufman 自适应均线
            "MAMA": lambda x, f, s: talib.MAMA(x, f, s),  # MESA 自适应均线
            # 动量类
            "RSI": lambda x, n: talib.RSI(x, n),
            "MACD": lambda c, f, s, n: talib.MACD(c, f, s, n),
            "KDJ": lambda h, l, c, fk, sk, sd: talib.STOCH(h, l, c, fk, sk, sd),
            "ADX": lambda h, l, c, n: talib.ADX(h, l, c, n),   # 趋势强度
            "CCI": lambda h, l, c, n: talib.CCI(h, l, c, n),
            "MOM": lambda c, n: talib.MOM(c, n),    # 动量
            "ROC": lambda c, n: talib.ROC(c, n),    # 变化率
            # 波动率类
            "ATR": lambda h, l, c, n: talib.ATR(h, l, c, n),
            "TRANGE": lambda h, l, c: talib.TRANGE(h, l, c),    # 真实波幅
            # 价量
            "OBV": lambda c, v: talib.OBV(c, v),        # 能量潮
            "AD": lambda h, l, c, v: talib.AD(h, l, c, v),  # Chaikin A/D 线
            "ADOSC": lambda h, l, c, v, f, s: talib.ADOSC(h, l, c, v, f, s),
            "MFI": lambda h, l, c, v, n: talib.MFI(h, l, c, v, n),   # 资金流量指数
            # 通道类
            "BBANDS": lambda c, n, u, d, t: talib.BBANDS(c, n, u, d, t),  # 布林带
            "SAR": lambda h, l, a, m: talib.SAR(h, l, a, m),  # 抛物线指标
            # 统计函数
            "CORREL": lambda x, y, n: talib.CORREL(x, y, n),    # 相关系数
            "STDDEV": lambda x, n: talib.STDDEV(x, n),    # 标准差
            "VAR": lambda x, n, d: talib.VAR(x, n, d),    # 方差

            # 扩展运算
            "LOG": np.log, "EXP": np.exp, "SQRT": np.sqrt, "POW": np.power, "ABS": np.abs,

            # 金融指标扩展
            "LLV": lambda x, n: x.rolling(n).min(),
            "HHV": lambda x, n: x.rolling(n).max(),
        }

    def load_set_from_file(self, path: str):
        from pydantic import ValidationError
        with open(path, "r") as f:
            data = json.load(f)
        try:
            indicator_set = IndicatorSet(**data)
        except ValidationError as e:
            logger.error(f"❌ 文件 {path} 验证失败:\n{e}")
            return
        self.sets[indicator_set.set_name] = {
            ind.name: ind.formula for ind in indicator_set.indicators
        }
        logger.info(f"✅ 已加载指标集 {indicator_set.set_name}")

    def calculate_set(self, df: pd.DataFrame, set_name: str) -> pd.DataFrame:
        if set_name not in self.sets:
            raise ValueError(f"指标集 {set_name} 未加载")
        result = df.copy()
        context = dict(self.context_base)
        context.update({
            "OPEN": df["open"], "HIGH": df["high"],
            "LOW": df["low"], "CLOSE": df["close"],
            "VOL": df["volume"],
        })
        for name, formula in self.sets[set_name].items():
            try:
                result[name] = eval(formula, {"__builtins__": None}, context)
                context[name] = result[name]   # 允许公式引用前面计算的指标
            except Exception as e:
                logger.error(f"⚠️ {set_name}.{name} 计算失败: {formula} -> {e}")
        return result


class IndicatorManager:
    def __init__(self):
        os.makedirs(INDS_DIR, exist_ok=True)
        self.indicators_dir = INDS_DIR
        self.engine = IndicatorEngine()

    def load_all_sets(self):
        """批量加载目录下的所有指标集"""
        logger.info(f"指标文件路径 {self.indicators_dir}")
        for fname in os.listdir(self.indicators_dir):
            if fname.endswith(".json"):
                path = os.path.join(self.indicators_dir, fname)
                self.engine.load_set_from_file(path)
                

    def list_sets(self) -> List[str]:
        """列出已加载的指标集名字"""
        return list(self.engine.sets.keys())

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        
        """计算所有启用的指标"""
        result = df.copy()
        for set_name in self.engine.sets.keys():
            result = self.engine.calculate_set(result, set_name)
            logger.info(f"计算指标集{set_name}")
        return result


# 通达信 / TradingView 变量映射表
VAR_MAP = {
    "C": "CLOSE",
    "H": "HIGH",
    "L": "LOW",
    "O": "OPEN",
    "V": "VOL",
}

def normalize_formula(formula: str) -> str:
    """把通达信/TradingView风格的公式转换成标准引擎公式"""
    # 去掉多余空格
    formula = formula.strip().upper()

    # 替换变量
    for k, v in VAR_MAP.items():
        # 避免部分函数名中包含字母冲突，加边界
        formula = re.sub(rf"\b{k}\b", v, formula)

    return formula

def formulas_to_json(set_name: str, indicators: Dict[str, str], out_path: str):
    """将指标字典转存为 JSON 文件"""
    ind_list = []
    for name, formula in indicators.items():
        ind_list.append({
            "name": name.upper(),
            "formula": normalize_formula(formula),
        })
    data = {"set_name": set_name, "indicators": ind_list}
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"✅ 已生成指标集 {set_name} -> {out_path}")
