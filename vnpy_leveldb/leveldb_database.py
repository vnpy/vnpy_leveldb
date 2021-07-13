""""""
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

path = str(get_file_path("leveldb"))
db = plyvel.DB(path, create_if_missing=True)


class LeveldbDatabase(BaseDatabase):
    """"""

    def __init__(self) -> None:
        """"""
        self.db = db

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """"""
        # save different exchange_symbol
        exchange_symbols = set()
        # Convert bar object to key:value structure and adjust timezone
        datas = []
        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)
            rowKey = "Bar" + "|" + bar.exchange.value + "-" + bar.symbol + "|" + bar.interval.value + "|" + bar.datetime.strftime("%Y-%m-%d %H:%M:%S")
            d = bar.__dict__
            d.pop("gateway_name")
            d.pop("vt_symbol")
            d.pop("exchange")
            d.pop("symbol")
            d.pop("datetime")
            rowValue = pickle.dumps(d)
            datas.append((rowKey, rowValue))

        # upsert data into database
        wb = self.db.write_batch()
        for data in datas:
            wb.put(data[0].encode(), data[1])
            exchange_symbols.add(data[0].split("|")[1])
        wb.write()

        # update bar overview
        for exchange_symbol in exchange_symbols:
            bar_overview_key = "BarOverview" + "|" + exchange_symbol + "|" + bar.interval.value
            bar_prefix = "Bar" + "|" + exchange_symbol + "|" + bar.interval.value + "|"
            sub_db = db.prefixed_db(bar_prefix.encode())
            bars = []
            for key, value in sub_db:
                bars.append((key, value))
            count = len(bars)
            start = bars[0][0].decode()
            end = bars[-1][0].decode()
            overview = BarOverview()
            overview.count = count
            overview.start = start
            overview.end = end
            d = overview.__dict__
            overview_value = pickle.dumps(d)
            db.put(bar_overview_key.encode(), overview_value)

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        # Convert bar object to key:value structure and adjust timezone
        datas = []
        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)
            rowKey = "Tick" + "|" + tick.exchange.value + "-" + tick.symbol + "|" + tick.datetime.strftime("%Y-%m-%d %H:%M:%S")
            d = tick.__dict__
            d.pop("gateway_name")
            d.pop("vt_symbol")
            d.pop("exchange")
            d.pop("symbol")
            d.pop("datetime")
            rowValue = pickle.dumps(d)
            datas.append((rowKey, rowValue))

        # upsert data into database
        wb = self.db.write_batch()
        for data in datas:
            wb.put(data[0].encode(), data[1])
        wb.write()

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        bars: List[BarData] = []
        vt_symbol = f"{symbol}.{exchange.value}"
        # pre库
        prefix = "Bar" + "|" + exchange.value + "-" + symbol + "|" + interval.value + "|"
        sub_db = db.prefixed_db(prefix.encode())
        start_time = start.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end.strftime("%Y-%m-%d %H:%M:%S")
        for key, value in sub_db.iterator(start=start_time.encode(), include_start=True,
                                          stop=end_time.encode(), include_stop=True):
            time = key.decode()
            row = pickle.loads(value)

            date_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
            date_time = DB_TZ.localize(date_time)
            tmp = BarData("DB", symbol, exchange, date_time)
            tmp.volume = row["volume"]
            tmp.open_interest = row["open_interest"]
            tmp.open_price = row["open_price"]
            tmp.high_price = row["high_price"]
            tmp.low_price = row["low_price"]
            tmp.close_price = row["close_price"]
            tmp.interval = interval
            tmp.vt_symbol = vt_symbol

            bars.append(tmp)
        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        ticks: List[TickData] = []
        vt_symbol = f"{symbol}.{exchange.value}"
        # pre库
        prefix = "Tick" + "|" + exchange.value + "-" + symbol + "|"
        sub_db = db.prefixed_db(prefix.encode())
        start_time = start.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end.strftime("%Y-%m-%d %H:%M:%S")
        for key, value in sub_db.iterator(start=start_time.encode(), include_start=True,
                                          stop=end_time.encode(), include_stop=True):
            time = key.decode()
            row = pickle.loads(value)

            date_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
            date_time = DB_TZ.localize(date_time)
            tmp = TickData("DB", symbol, exchange, date_time)

            tmp.name, tmp.volume, tmp.open_interest = row["name"], row["volume"], row["open_interest"]
            tmp.last_price, tmp.last_volume = row["last_price"], row["last_volume"]
            tmp.limit_up, tmp.limit_down = row["limit_up"], row["limit_down"]
            tmp.open_price, tmp.high_price = row["open_price"], row["high_price"]
            tmp.low_price, tmp.pre_close = row["low_price"], row["pre_close"]
            tmp.bid_price_1, tmp.bid_price_2 = row["bid_price_1"], row["bid_price_2"]
            tmp.bid_price_3, tmp.bid_price_4 = row["bid_price_3"], row["bid_price_4"]
            tmp.bid_price_5 = row["bid_price_5"]
            tmp.ask_price_1, tmp.ask_price_2 = row["ask_price_1"], row["ask_price_2"]
            tmp.ask_price_3, tmp.ask_price_4 = row["ask_price_3"], row["ask_price_4"]
            tmp.ask_price_5 = row["ask_price_5"]
            tmp.bid_volume_1, tmp.bid_volume_2 = row["bid_volume_1"], row["bid_volume_2"]
            tmp.bid_volume_3, tmp.bid_volume_4 = row["bid_volume_3"], row["bid_volume_4"]
            tmp.bid_volume_5 = row["bid_volume_5"]
            tmp.ask_volume_1, tmp.ask_volume_2 = row["ask_volume_1"], row["ask_volume_2"]
            tmp.ask_volume_3, tmp.ask_volume_4 = row["ask_volume_3"], row["ask_volume_4"]
            tmp.ask_volume_5 = row["ask_volume_5"]

            tmp.vt_symbol = vt_symbol

            ticks.append(tmp)
        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        prefix = "Bar" + "|" + exchange.value + "-" + symbol + "|" + interval.value + "|"
        sub_db = db.prefixed_db(prefix.encode())
        count = 0
        for key, value in sub_db.iterator():
            sub_db.delete(key)
            count += 1

        # Delete bar overview
        exchange_symbol = exchange.value + "-" + symbol
        bar_overview_key = "BarOverview" + "|" + exchange_symbol + "|" + interval.value
        db.delete(bar_overview_key.encode())

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        prefix = "Tick" + "|" + exchange.value + "-" + symbol + "|"
        sub_db = db.prefixed_db(prefix.encode())
        count = 0
        for key, value in sub_db.iterator():
            sub_db.delete(key)
            count += 1

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """"""
        prefix = "BarOverview" + "|"
        data = []
        sub_db = db.prefixed_db(prefix.encode())
        for key, value in sub_db:
            value = pickle.loads(value)
            data.append((key, value))
        return data


database_manager = LeveldbDatabase()
