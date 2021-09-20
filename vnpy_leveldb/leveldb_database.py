from datetime import datetime
from typing import List
import pickle

import plyvel

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import get_file_path
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


class LeveldbDatabase(BaseDatabase):
    """LevelDB数据库接口"""

    def __init__(self) -> None:
        """"""
        filename: str = SETTINGS["database.database"]
        filepath: str = str(get_file_path(filename))

        self.db: plyvel.DB = plyvel.DB(filepath, create_if_missing=True)

        self.bar_db: plyvel.DB = self.db.prefixed_db(b"bar-")
        self.tick_db: plyvel.DB = self.db.prefixed_db(b"tick-")
        self.overview_db: plyvel.DB = self.db.prefixed_db(b"overview-")

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存K线数据"""
        # 获取子数据库
        bar: BarData = bars[0]
        prefix = generate_bar_prefix(bar.symbol, bar.exchange, bar.interval)
        db: plyvel.DB = self.bar_db.prefixed_db(prefix.encode())

        # 批量写入数据
        with db.write_batch() as wb:
            for bar in bars:
                key = str(bar.datetime).encode()
                value = pickle.dumps(bar)
                wb.put(key, value)
            wb.write()

        # # 更新K线汇总数据
        # bar_overview_key = "BarOverview" + "|" + exchange_symbol + "|" + interval
        # bar_prefix = "Bar" + "|" + exchange_symbol + "|" + interval + "|"
        # sub_db = self.db.prefixed_db(bar_prefix.encode())
        # loaded_bars: List[tuple] = []
        # for key, value in sub_db:
        #     loaded_bars.append((key, value))
        # count = len(bars)
        # start = loaded_bars[0][0].decode()
        # end = loaded_bars[-1][0].decode()
        # overview = BarOverview(symbol, Exchange(exchange), Interval(interval))
        # overview.count = count
        # overview.start = start
        # overview.end = end
        # d = overview.__dict__
        # overview_value = pickle.dumps(d)
        # self.db.put(bar_overview_key.encode(), overview_value)

        return True

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        # 获取子数据库
        tick: TickData = ticks[0]
        prefix = generate_tick_prefix(tick.symbol, tick.exchange)
        db: plyvel.DB = self.tick_db.prefixed_db(prefix.encode())

        # 批量写入数据
        with db.write_batch() as wb:
            for tick in ticks:
                key = str(tick.datetime).encode()
                value = pickle.dumps(tick)
                wb.put(key, value)
            wb.write()

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
        prefix = generate_bar_prefix(symbol, exchange, interval)
        db: plyvel.DB = self.bar_db.prefixed_db(prefix.encode())

        # 读取数据
        bars: List[BarData] = []

        for _, value in db.iterator(
            start=str(start).encode(),
            stop=str(end).encode(),
            include_start=True,
            include_stop=True
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
        prefix = generate_tick_prefix(symbol, exchange)
        db: plyvel.DB = self.tick_db.prefixed_db(prefix.encode())

        # 读取数据
        ticks: List[TickData] = []

        for _, value in db.iterator(
            start=str(start).encode(),
            stop=str(end).encode(),
            include_start=True,
            include_stop=True
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
        prefix = generate_bar_prefix(symbol, exchange, interval)
        db: plyvel.DB = self.bar_db.prefixed_db(prefix.encode())
        
        # 遍历删除
        count = 0

        with db.write_batch() as wb:
            for key, _  in db.iterator():
                count += 1
                wb.delete(key)

            wb.write()

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        # 获取子数据库
        prefix = generate_tick_prefix(symbol, exchange)
        db: plyvel.DB = self.tick_db.prefixed_db(prefix.encode())
        
        # 遍历删除
        count = 0

        with db.write_batch() as wb:
            for key, _  in db.iterator():
                count += 1
                wb.delete(key)

            wb.write()

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """查询数据库中的K线汇总信息"""
        prefix = "BarOverview" + "|"
        data: List[BarOverview] = []
        sub_db = self.db.prefixed_db(prefix.encode())
        for key, value in sub_db:
            value = pickle.loads(value)
            key = key.decode()
            symbol = value["symbol"]
            exchange = value["exchange"].value
            interval = value["interval"].value
            count = value["count"]
            start = value["start"]
            start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            end = value["end"]
            end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            overview = BarOverview(symbol, Exchange(exchange), Interval(interval), count, start, end)
            data.append(overview)
        return data


def generate_bar_prefix(symbol: str, exchange: Exchange, interval: Interval) -> str:
    """生成K线数据前缀"""
    return f"{interval.value}-{exchange.value}-{symbol}"


def generate_tick_prefix(symbol: str, exchange: Exchange) -> str:
    """生成Tick数据前缀"""
    return f"{exchange.value}-{symbol}"
