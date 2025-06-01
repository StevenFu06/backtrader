import backtrader as bt
from ib_async import IB, Stock, MarketOrder, LimitOrder, Forex, StopOrder, Future
from ibastore import IBAStore
from ibabroker import IBBroker
import datetime


host = "192.168.0.163"
port = 4002
clientId = 1


class IBData(bt.feeds.DataBase):
    # Note for "ones" i.e. 1 min or 1 days it should be min/day. So be wary of this in future.
    # The exception is second. For some reason its still secs.
    TF_CONVERSION = {
        bt.TimeFrame.Seconds: {"to_sec": 1, "to_letter": "S", "to_word": "secs"},
        bt.TimeFrame.Minutes: {"to_sec": 60, "to_letter": "M", "to_word": "mins"},
        bt.TimeFrame.Days: {"to_sec": 86400, "to_letter": "D", "to_word": "days"},
        bt.TimeFrame.Weeks: {"to_sec": 604800, "to_letter": "W", "to_word": "weeks"},
        bt.TimeFrame.Months: {"to_sec": 2592000, "to_letter": "M", "to_word": "months"},
    }

    params = (
        # Dataname is ticker because ibkr tends to fill contract with extra parameters.
        # Makes it more consistant with backtesting too. Also note that its mainly used for
        # getting open positions so it should be named what IBKR calls it
        ("dataname", None),
        # either contract from IBKR or manually defined
        ("tradecontract", None),
        ("ib", None),
        ("whatToShow", "MIDPOINT"),
        ("useRTH", True),
        ("num_hist_bars", None),
        ("timeframe", bt.TimeFrame.Minutes),
        ("compression", 15),
        ("refresh_rate", 0.01),
    )

    def __init__(self):
        super().__init__()

        # Data storage variables
        self.live_data = None
        self.hist_data = None

        # Data tracking variables
        self._laststatus = self.CONNECTED
        self._haslivedata = False
        self.last_runtime = None
        self.last_data_time = None

        # for forming ohclv data
        self._open = []
        self._high = []
        self._low = []
        self._close = []
        self._volume = []

    def start(self):
        # We track the start time of algo
        self.last_runtime = datetime.datetime.now()
        # Subscribe to market data
        self.live_data = self.p.ib.reqRealTimeBars(
            self.p.tradecontract, barSize=5, whatToShow=self.p.whatToShow, useRTH=self.p.useRTH
        )
        # Get historical Data
        if self.p.num_hist_bars is not None:
            tf_info = self.TF_CONVERSION[self.p.timeframe]
            duration = f"{self.p.num_hist_bars * self.p.compression} {tf_info['to_letter']}"
            bar_size = f"{self.p.compression} {tf_info['to_word']}"

            self.hist_data = self.p.ib.reqHistoricalData(
                self.p.contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=self.p.whatToShow,
                useRTH=self.p.useRTH,
                formatDate=1,
            )

    def new_data_available(self):
        if not self.live_data:
            return False
        elif self.last_data_time is None:
            self.last_data_time = self.live_data[-1].time
            return True
        elif self.last_data_time != self.live_data[-1].time:
            self.last_data_time = self.live_data[-1].time
            return True
        return False

    def _load(self):
        """
        Note for me in future, if broker is not initiated, we need to add a ib.sleep. Usually controlled by broker.
        This is because next in broker gets called every cycle unlike strategy.
        """
        # self.p.ib.sleep(self.p.refresh_rate)
        # Preload in the historical data setting all _load to true. This will help prime the algo.
        if self.hist_data:
            self.lines.datetime[0] = bt.date2num(self.hist_data[0].date)
            self.lines.open[0] = self.hist_data[0].open
            self.lines.high[0] = self.hist_data[0].high
            self.lines.low[0] = self.hist_data[0].low
            self.lines.close[0] = self.hist_data[0].close
            self.lines.volume[0] = self.hist_data[0].volume
            self.hist_data.pop(0)
            return True

        # If we get past hist_data we are now live
        self._laststatus = self.LIVE
        if not self.bar_complete() and self.new_data_available():
            self._open.append(self.live_data[-1].open_)
            self._high.append(self.live_data[-1].high)
            self._low.append(self.live_data[-1].low)
            self._close.append(self.live_data[-1].close)
            self._volume.append(self.live_data[-1].volume)
            return None

        elif self.bar_complete():
            # Snap to 0,0 because my xgboost uses this
            self.lines.datetime[0] = bt.date2num(self.last_runtime.replace(second=0, microsecond=0))
            self.lines.open[0] = self._open[0]
            self.lines.high[0] = max(self._high) if self._high else None
            self.lines.low[0] = min(self._low) if self._low else None
            self.lines.close[0] = self._close[-1] if self._close else None
            self.lines.volume[0] = sum(self._volume) / len(self._volume) if self._volume else None

            # Crucial because we update this outside of _bar_complete
            self.last_runtime = datetime.datetime.now()
            # We now have our first bar
            self._haslivedata = True

            # Clear lists for next bar
            self._open.clear()
            self._high.clear()
            self._low.clear()
            self._close.clear()
            self._volume.clear()
            return True
        return None

    def bar_complete(self):
        now = datetime.datetime.now()
        interval_seconds = self.TF_CONVERSION[self.p.timeframe]["to_sec"] * self.p.compression
        elapsed = (now - self.last_runtime).total_seconds()
        return True if elapsed >= interval_seconds else False

    def haslivedata(self):
        """For telling backtrader we gucci we now have live data you can continue"""
        return self._haslivedata

    def islive(self):
        """For telling backtrader this is live"""
        return True


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
        print(self.data.close[0])
        print(self.broker.getvalue())
        self.buy(data=self.data, size=10)
        print("we done")

    def notify_trade(self, trade):
        print("Not implemented")

    def notify_order(self, order):
        print("-" * 100)
        print("we orderd boi")
        print(order.data.p.tradecontract)
        print(order.executed.value)
        print(order.status == bt.Order.Canceled)
        print("-" * 100)


# contract = Stock(symbol="AMD", currency="USD", exchange="SMART")
# contract = Future(symbol="GC", lastTradeDateOrContractMonth="202506", exchange="COMEX", currency="USD")
contract = Forex("USDCAD")

# store = IBAStore(host=host, port=port, clientId=clientId, refreshrate=0.05)
# cerebro = bt.Cerebro()
# data = IBData(ib=store.conn, dataname="CL", tradecontract=contract, compression=5, timeframe=bt.TimeFrame.Seconds)
# cerebro.broker = store.getbroker()
# cerebro.addstrategy(RandomStrategy)
# cerebro.adddata(data)
# cerebro.run()
# print(store.getposition("USD"))


ib = IB()
ib.connect(host, port, clientId)
bars = ib.reqRealTimeBars(contract, barSize=5, whatToShow="MIDPOINT", useRTH=True)
for i in range(10):
    print(bars)
    ib.sleep(2.5)
