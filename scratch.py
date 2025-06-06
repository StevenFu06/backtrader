import backtrader as bt
from ib_async import IB, Stock, MarketOrder, Forex, StopOrder, Future
from ibastore import IBAStore

# Note even if not needed directly need to import to set brokercls and datacls
from ibabroker import IBABroker
from ibadata import IBAData
import datetime
from datetime import timedelta


host = "192.168.0.163"
port = 4002
clientId = 1
stock_con = Stock(symbol="AMD", currency="USD", exchange="SMART")
future_con = Future(symbol="GC", lastTradeDateOrContractMonth="202506", exchange="COMEX", currency="USD")
forex_con = Forex("USDCAD")


class RandomStrategy(bt.Strategy):
    def __init__(self):
        print("Random strategy initialized")

    def next(self):
        """
        In live trading I need to make sure all the data is in before runing my model. I think this is fine?
        Even if delayed because backtrader is running off the internal clock using "last_runtime" it should
        just grab to the nearest 5 seconds. But Need to test for this just in case.

        After more pondering, I think they should always be alinged as long as their compression and timeframe
        matches. HOWEVER I need to be extra careful of then and just watch to see double next actvations.
        """
        print(f"[DEBUG] Datetime: {self.data.datetime.datetime(0)}, Close: {self.data.close[0]}")

    def notify_trade(self, trade):
        print("Not implemented")

    def notify_order(self, order):
        print("-" * 100)
        print("we orderd boi")
        print(order.data.p.tradecontract)
        print(order.executed.value)
        print(order.status == bt.Order.Canceled)
        print("-" * 100)


def run_ibkr():
    from zoneinfo import ZoneInfo

    ib = IB()
    ib.connect(host, port, clientId)
    # ticker = ib.reqMktData(future_con)
    # ticker = ib.reqRealTimeBars(future_con, barSize=5, whatToShow="BID", useRTH=False)
    utc = ZoneInfo("UTC")
    fromdate = datetime.datetime.now(utc) - timedelta(seconds=20)
    ticker = ib.reqHistoricalData(
        forex_con,
        endDateTime=fromdate,
        durationStr="20 D",
        barSizeSetting="15 mins",
        whatToShow="BID",
        useRTH=True,
        formatDate=1,
    )
    while True:
        ib.sleep(1)
        print(ticker)


def main():
    from zoneinfo import ZoneInfo

    store = IBAStore(host=host, port=port, clientId=clientId, timeoffset=True)
    cerebro = bt.Cerebro()

    # cerebro.broker = store.getbroker()
    utc = ZoneInfo("UTC")
    fromdate = datetime.datetime.now() - timedelta(days=2)
    data = store.getdata(
        dataname="USD.CAD-CASH-IDEALPRO",
        # fromdate=fromdate,
        rtbar=False,
        backfill_start=False,
        backfill=False,
        timeframe=bt.TimeFrame.Minutes,
        compression=15,
    )
    cerebro.resampledata(data, timeframe=bt.TimeFrame.Seconds, compression=3)
    cerebro.addstrategy(RandomStrategy)
    cerebro.adddata(data)
    cerebro.run()


if __name__ == "__main__":
    main()
    # run_ibkr()
