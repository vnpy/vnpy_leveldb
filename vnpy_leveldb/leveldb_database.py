from datetime import datetime
from typing import List
import pickle

from plyvel import DB

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import get_file_path
from vnpy.trader.database import BaseDatabase, BarOverview, TickOverview, DB_TZ
from vnpy.trader.setting import SETTINGS


class LeveldbDatabase(BaseDatabase):
    """LevelDB数据库接口"""

    def __init__(self) -> None:
        """"""
        filename: str = SETTINGS["database.database"]
        filepath: str = str(get_file_path(filename))

        self.db: DB = DB(filepath, create_if_missing=True)

        self.bar_db: DB = self.db.prefixed_db(b"bar|")
        self.tick_db: DB = self.db.prefixed_db(b"tick|")
        self.bar_overview_db: DB = self.db.prefixed_db(b"bar_overview|")
        self.tick_overview_db: DB = self.db.prefixed_db(b"tick_overview|")

    def save_bar_data(self, bars: List[BarData], stream: bool = False) -> bool:
        """保存K线数据"""
        # 获取子数据库
        bar: BarData = bars[0]
        prefix: str = generate_bar_prefix(bar.symbol, bar.exchange, bar.interval)
        db: DB = self.bar_db.prefixed_db(prefix.encode())

        # 批量写入数据
        with db.write_batch() as wb:
            for bar in bars:
                bar.datetime = bar.datetime.astimezone(DB_TZ)       # 转换时区
                key: str = str(bar.datetime).encode()
                value: bytes = pickle.dumps(bar)
                wb.put(key, value)

            wb.write()

        # 更新K线汇总数据
        buf = self.bar_overview_db.get(prefix.encode())

        if not buf:
            overview: BarOverview = BarOverview(
                symbol=bar.symbol,
                exchange=bar.exchange,
                interval=bar.interval,
                count=len(bars),
                start=bars[0].datetime,
                end=bars[-1].datetime
            )
        elif stream:
            overview: BarOverview = pickle.loads(buf)
            overview.end = bars[-1].datetime
            overview.count += len(bars)
        else:
            overview: BarOverview = pickle.loads(buf)
            overview.start = min(overview.start, bars[0].datetime)
            overview.end = max(overview.end, bars[-1].datetime)
            overview.count = len(list(db.iterator(include_value=False)))

        self.bar_overview_db.put(prefix.encode(), pickle.dumps(overview))

        return True

    def save_tick_data(self, ticks: List[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        # 获取子数据库
        tick: TickData = ticks[0]
        prefix: str = generate_tick_prefix(tick.symbol, tick.exchange)
        db: DB = self.tick_db.prefixed_db(prefix.encode())

        # 批量写入数据
        with db.write_batch() as wb:
            for tick in ticks:
                tick.datetime = tick.datetime.astimezone(DB_TZ)     # 转换时区
                key: str = str(tick.datetime).encode()
                value: bytes = pickle.dumps(tick)
                wb.put(key, value)
            wb.write()

        # 更新Tick汇总数据
        buf = self.tick_overview_db.get(prefix.encode())

        if not buf:
            overview: TickOverview = TickOverview(
                symbol=tick.symbol,
                exchange=tick.exchange,
                count=len(ticks),
                start=ticks[0].datetime,
                end=ticks[-1].datetime
            )
        elif stream:
            overview: BarOverview = pickle.loads(buf)
            overview.end = ticks[-1].datetime
            overview.count += len(ticks)
        else:
            overview: TickOverview = pickle.loads(buf)
            overview.start = min(overview.start, ticks[0].datetime)
            overview.end = max(overview.end, ticks[-1].datetime)
            overview.count = len(list(db.iterator(include_value=False)))

        self.tick_overview_db.put(prefix.encode(), pickle.dumps(overview))

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """读取K线数据"""
        # 获取子数据库
        prefix: str = generate_bar_prefix(symbol, exchange, interval)
        db: DB = self.bar_db.prefixed_db(prefix.encode())

        # 读取数据
        bars: List[BarData] = []

        for value in db.iterator(
            start=str(start).encode(),
            stop=str(end).encode(),
            include_start=True,
            include_stop=True,
            include_key=False
        ):
            bar: BarData = pickle.loads(value)
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """读取TICK数据"""
        # 获取子数据库
        prefix: str = generate_tick_prefix(symbol, exchange)
        db: DB = self.tick_db.prefixed_db(prefix.encode())

        # 读取数据
        ticks: List[TickData] = []

        for value in db.iterator(
            start=str(start).encode(),
            stop=str(end).encode(),
            include_start=True,
            include_stop=True,
            include_key=False
        ):
            tick: TickData = pickle.loads(value)
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        # 获取子数据库
        prefix: str = generate_bar_prefix(symbol, exchange, interval)
        db: DB = self.bar_db.prefixed_db(prefix.encode())

        # 遍历删除
        count: int = 0

        with db.write_batch() as wb:
            for key in db.iterator(include_value=False):
                count += 1
                wb.delete(key)

            wb.write()

        # 删除汇总
        self.bar_overview_db.delete(prefix.encode())

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        # 获取子数据库
        prefix: str = generate_tick_prefix(symbol, exchange)
        db: DB = self.tick_db.prefixed_db(prefix.encode())

        # 遍历删除
        count: int = 0

        with db.write_batch() as wb:
            for key in db.iterator(include_value=False):
                count += 1
                wb.delete(key)

            wb.write()

        # 删除汇总
        self.tick_overview_db.delete(prefix.encode())

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """查询数据库中的K线汇总信息"""
        overviews: List[BarOverview] = []

        for value in self.bar_overview_db.iterator(include_key=False):
            overview: BarOverview = pickle.loads(value)
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> List[TickOverview]:
        """查询数据库中的K线汇总信息"""
        overviews: List[TickOverview] = []

        for value in self.tick_overview_db.iterator(include_key=False):
            overview: TickOverview = pickle.loads(value)
            overviews.append(overview)

        return overviews


def generate_bar_prefix(symbol: str, exchange: Exchange, interval: Interval) -> str:
    """生成K线数据前缀"""
    return f"{interval.value}|{exchange.value}|{symbol}|"


def generate_tick_prefix(symbol: str, exchange: Exchange) -> str:
    """生成Tick数据前缀"""
    return f"{exchange.value}|{symbol}|"
