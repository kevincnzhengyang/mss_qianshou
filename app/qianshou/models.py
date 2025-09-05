'''
Author: kevincnzhengyang kevin.cn.zhengyang@gmail.com
Date: 2025-08-27 21:12:28
LastEditors: kevincnzhengyang kevin.cn.zhengyang@gmail.com
LastEditTime: 2025-09-05 18:51:47
FilePath: /mss_qianshou/app/qianshou/models.py
Description: 数据模型

Copyright (c) 2025 by ${git_name_email}, All Rights Reserved. 
'''

from typing import List
from pydantic import BaseModel, Field

class Equity(BaseModel):
    id: int | None = None
    symbol: str
    market: str         # us, sh, sz, hk, tw, tokyo, london
    note: str = ""
    enabled: bool = True
    last_date: str  | None = None
    updated_at: str | None = None

    def to_yfinance_symbol(self) -> str:
        """转换为yfinance格式的symbol"""
        if self.market == "US":
            return self.symbol
        elif self.market == "SH":
            return f"{self.symbol}.SS"
        elif self.market in ["SZ", "HK", "TW"]:
            return f"{self.symbol}.{self.market}"
        elif self.market == "TOKYO":
            return f"{self.symbol}.T"
        elif self.market == "LONDON":
            return f"{self.symbol}.L"
        else:
            raise ValueError(f"YFinance不支持的市场: {self.market}")
    
    def to_futu_symbol(self) -> str:
        """转换为FutuOpenAPI的symbol"""
        if self.symbol == "^HSI":
            return "HK.800000"
        else:
            if self.market in ["US", "TW", "TOKYO", "LONDON"]:
                return f"{self.market}.{self.symbol}"
            elif self.market in ["SH", "SZ"]:
                return f"{self.market}.{self.symbol.rjust(6, '0')}"
            elif self.market == "HK":
                return f"HK.{self.symbol.rjust(5, '0')}"
            else:
                raise ValueError(f"FutuAPI不支持的市场: {self.market}")
            
    def to_akshare_name(self) -> str:
        """转换为AkShare的symbol"""
        if self.market in ["SH", "SZ"]:
            return f"{self.symbol.rjust(6, '0')}"
        elif self.market == "HK":
            return f"{self.symbol.rjust(5, '0')}"
        else:
            raise ValueError(f"AkShare不支持的市场: {self.market}")

class IndicatorDef(BaseModel):
    name: str = Field(..., pattern=r"^[A-Z0-9_]+$")  # type: ignore
    description: str = ""
    formula: str

class IndicatorSet(BaseModel):
    set_name: str
    indicators: List[IndicatorDef]
    