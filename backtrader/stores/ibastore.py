import collections
from copy import copy
import random
from datetime import datetime, timedelta, timezone

from ib_async import IB, Contract
from backtrader import Position, TimeFrame
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass, queue
from backtrader.utils import AutoDict



class MetaSingleton(MetaParams):
    """Metaclass to make a metaclassed class a singleton"""

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super(MetaSingleton, cls).__call__(*args, **kwargs)

        return cls._singleton


class IBAStore(with_metaclass(MetaSingleton, object)):
    # The _durations are meant to calculate the needed historical data to
    # perform backfilling at the start of a connetion or a connection is lost.
    # Using a timedelta as a key allows to quickly find out which bar size
    # bar size (values in the tuples int the dict) can be used.

    _durations = dict(
        [
            # 60 seconds - 1 min
            ("60 S", ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min")),
            # 120 seconds - 2 mins
            ("120 S", ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins")),
            # 180 seconds - 3 mins
            ("180 S", ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins")),
            # 300 seconds - 5 mins
            ("300 S", ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins", "5 mins")),
            # 600 seconds - 10 mins
            (
                "600 S",
                ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins", "5 mins", "10 mins"),
            ),
            # 900 seconds - 15 mins
            (
                "900 S",
                (
                    "1 secs",
                    "5 secs",
                    "10 secs",
                    "15 secs",
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                ),
            ),
            # 1200 seconds - 20 mins
            (
                "1200 S",
                (
                    "1 secs",
                    "5 secs",
                    "10 secs",
                    "15 secs",
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                ),
            ),
            # 1800 seconds - 30 mins
            (
                "1800 S",
                (
                    "1 secs",
                    "5 secs",
                    "10 secs",
                    "15 secs",
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                ),
            ),
            # 3600 seconds - 1 hour
            (
                "3600 S",
                (
                    "5 secs",
                    "10 secs",
                    "15 secs",
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                ),
            ),
            # 7200 seconds - 2 hours
            (
                "7200 S",
                (
                    "5 secs",
                    "10 secs",
                    "15 secs",
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                    "2 hours",
                ),
            ),
            # 10800 seconds - 3 hours
            (
                "10800 S",
                (
                    "10 secs",
                    "15 secs",
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                    "2 hours",
                    "3 hours",
                ),
            ),
            # 14400 seconds - 4 hours
            (
                "14400 S",
                (
                    "15 secs",
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                    "2 hours",
                    "3 hours",
                    "4 hours",
                ),
            ),
            # 28800 seconds - 8 hours
            (
                "28800 S",
                (
                    "30 secs",
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                    "2 hours",
                    "3 hours",
                    "4 hours",
                    "8 hours",
                ),
            ),
            # 1 days
            (
                "1 D",
                (
                    "1 min",
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                    "2 hours",
                    "3 hours",
                    "4 hours",
                    "8 hours",
                    "1 day",
                ),
            ),
            # 2 days
            (
                "2 D",
                (
                    "2 mins",
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                    "2 hours",
                    "3 hours",
                    "4 hours",
                    "8 hours",
                    "1 day",
                ),
            ),
            # 1 weeks
            (
                "1 W",
                (
                    "3 mins",
                    "5 mins",
                    "10 mins",
                    "15 mins",
                    "20 mins",
                    "30 mins",
                    "1 hour",
                    "2 hours",
                    "3 hours",
                    "4 hours",
                    "8 hours",
                    "1 day",
                    "1 W",
                ),
            ),
            # 2 weeks
            (
                "2 W",
                ("15 mins", "20 mins", "30 mins", "1 hour", "2 hours", "3 hours", "4 hours", "8 hours", "1 day", "1 W"),
            ),
            # 1 months
            ("1 M", ("30 mins", "1 hour", "2 hours", "3 hours", "4 hours", "8 hours", "1 day", "1 W", "1 M")),
            # 2+ months
            ("2 M", ("1 day", "1 W", "1 M")),
            ("3 M", ("1 day", "1 W", "1 M")),
            ("4 M", ("1 day", "1 W", "1 M")),
            ("5 M", ("1 day", "1 W", "1 M")),
            ("6 M", ("1 day", "1 W", "1 M")),
            ("7 M", ("1 day", "1 W", "1 M")),
            ("8 M", ("1 day", "1 W", "1 M")),
            ("9 M", ("1 day", "1 W", "1 M")),
            ("10 M", ("1 day", "1 W", "1 M")),
            ("11 M", ("1 day", "1 W", "1 M")),
            # 1+ years
            ("1 Y", ("1 day", "1 W", "1 M")),
        ]
    )

    # Sizes allow for quick translation from bar sizes above to actual
    # timeframes to make a comparison with the actual data
    _sizes = {
        "secs": (TimeFrame.Seconds, 1),
        "min": (TimeFrame.Minutes, 1),
        "mins": (TimeFrame.Minutes, 1),
        "hour": (TimeFrame.Minutes, 60),
        "hours": (TimeFrame.Minutes, 60),
        "day": (TimeFrame.Days, 1),
        "W": (TimeFrame.Weeks, 1),
        "M": (TimeFrame.Months, 1),
    }

    _dur2tf = {
        "S": TimeFrame.Seconds,
        "D": TimeFrame.Days,
        "W": TimeFrame.Weeks,
        "M": TimeFrame.Months,
        "Y": TimeFrame.Years,
    }

    # Set a base for the data requests (historical/realtime) to distinguish the
    # id in the error notifications from orders, where the basis (usually
    # starting at 1) is set by TWS
    REQIDBASE = 0x01000000

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = (
        ("host", "127.0.0.1"),
        ("port", 7496),
        ("clientId", None),  # None generates a random clientid 1 -> 2^16
        ("notifyall", False),
        ("_debug", False),
        ("refreshrate", 0.02),  # Refresh rate of ib_async in the form of ib.sleep
        ("reconnect", 3),  # -1 forever, 0 No, > 0 number of retries
        ("timeout", 3.0),  # timeout between reconnections
        ("timeoffset", True),  # Use offset to server for timestamps if needed
        ("timerefresh", 60.0),  # How often to refresh the timeoffset
        ("indcash", True),  # Treat IND codes as CASH elements
    )

    events = (
        "connectedEvent",
        "disconnectedEvent",
        "updateEvent",
        "pendingTickersEvent",
        "barUpdateEvent",
        "newOrderEvent",
        "orderModifyEvent",
        "cancelOrderEvent",
        "openOrderEvent",
        "orderStatusEvent",
        "execDetailsEvent",
        "commissionReportEvent",
        "updatePortfolioEvent",
        "positionEvent",
        "accountValueEvent",
        "accountSummaryEvent",
        "pnlEvent",
        "pnlSingleEvent",
        "scannerDataEvent",
        "tickNewsEvent",
        "newsBulletinEvent",
        "wshMetaEvent",
        "wshEvent",
        "errorEvent",
        "timeoutEvent",
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns ``DataCls`` with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered ``BrokerCls``"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(IBAStore, self).__init__()

        self.notifs = queue.Queue()  # store notifications for cerebro
        self.dontreconnect = False  # for non-recoverable connect errors
        # First instance that will call ib.sleep() will set this to true to prevent stacking
        self.refreshing = False

        self.broker = None  # broker instance
        self.datas = dict()  # data stored in dict instead of queue and get queue
        self._env = None  # reference to cerebro for general notifications

        # Create connection object
        self.conn = IB().connect(host=self.p.host, port=self.p.port, clientId=self.p.clientId)
        # Register them to async
        for event in self.events:
            getattr(self.conn, event).connect(getattr(self, event))
            if self.p.notifyall or self.p._debug:
                getattr(self.conn, event).connect(self.watcher)

        # Use the provided clientId or a random one
        if self.p.clientId is None:
            self.clientId = random.randint(1, pow(2, 16) - 1)
        else:
            self.clientId = self.p.clientId

        # This utility key function transforms a barsize into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def keyfn(x):
            n, t = x.split()
            tf, comp = self._sizes[t]
            return (tf, int(n) * comp)

        # This utility key function transforms a duration into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def key2fn(x):
            n, d = x.split()
            tf = self._dur2tf[d]
            return (tf, int(n))

        # Generate a table of reverse durations
        self.revdur = collections.defaultdict(list)
        # The table (dict) is a ONE to MANY relation of
        #   duration -> barsizes
        # Here it is reversed to get a ONE to MANY relation of
        #   barsize -> durations
        for duration, barsizes in self._durations.items():
            for barsize in barsizes:
                self.revdur[keyfn(barsize)].append(duration)

        # Once managed, sort the durations according to real duration and not
        # to the text form using the utility key above
        for barsize in self.revdur:
            self.revdur[barsize].sort(key=key2fn)

    def start(self, data=None, broker=None):
        self.reconnect(fromstart=True)  # reconnect should be an invariant
        if data is not None:
            self._env = data._env
            return [None]

        if broker is not None:
            self.broker = broker

    def poststart(self, data):
        # instead of saving this as seperate dict, we hold all datas in the datas class
        self.datas[data.contract.conId] = data

    def stop(self):
        try:
            self.conn.disconnect()  # disconnect should be an invariant
        except AttributeError:
            pass  # conn may have never been connected and lack "disconnect"

    def logmsg(self, *args):
        # for logging purposes
        if self.p._debug:
            print(*args)

    def watcher(self, *args):
        # will be registered to see all messages if debug is requested
        self.logmsg(*args)
        if self.p.notifyall:
            self.notifs.put(args)

    def connected(self):
        # The isConnected method is available through __getattr__ indirections
        # and may not be present, which indicates that no connection has been
        # made because the subattribute sender has not yet been created, hence
        # the check for the AttributeError exception
        try:
            return self.conn.isConnected()
        except AttributeError:
            pass

        return False  # non-connected (including non-initialized)

    def reconnect(self, fromstart=False, resub=False):
        # This method must be an invariant in that it can be called several
        # times from the same source and must be consistent. An exampler would
        # be 5 datas which are being received simultaneously and all request a
        # reconnect

        # Policy:
        #  - if dontreconnect has been set, no option to connect is possible
        #  - check connection and use the absence of isConnected as signal of
        #    first ever connection (add 1 to retries too)
        #  - Calculate the retries (forever or not)
        #  - Try to connct
        #  - If achieved and fromstart is false, the datas will be
        #    re-kickstarted to recreate the subscription
        firstconnect = False
        try:
            if self.conn.isConnected():
                if resub:
                    self.startdatas()
                return True  # nothing to do
        except AttributeError:
            # Not connected, several __getattr__ indirections to
            # self.conn.sender.client.isConnected
            firstconnect = True

        if self.dontreconnect:
            return False

        # This is only invoked from the main thread by datas
        retries = self.p.reconnect
        if retries >= 0:
            retries += firstconnect

        while retries < 0 or retries:
            if not firstconnect:
                # Sleep with ib.sleep to allow for background connection
                self.conn.sleep(self.p.timeout)

            firstconnect = False

            if self.conn.connect():
                if not fromstart or resub:
                    self.startdatas()
                return True  # connection successful

            if retries > 0:
                retries -= 1

        self.dontreconnect = True
        return False  # connection/reconnection failed

    def startdatas(self):
        for _, data in self.datas.items():
            data.reqdata()
        # Force an update
        self.conn.sleep(self.p.refreshrate)

    def stopdatas(self):
        for _, data in self.datas.items():
            data.canceldata()
        # Force an update
        self.conn.sleep(self.p.refreshrate)

    def get_notifications(self):
        """Return the pending "store" notifications"""
        # The background thread could keep on adding notifications. The None
        # mark allows to identify which is the last notification to deliver
        self.notifs.put(None)  # put a mark
        notifs = list()
        while True:
            notif = self.notifs.get()
            if notif is None:  # mark is reached
                break
            notifs.append(notif)

        return notifs

    def getAccountValues(self, tag, currency="BASE"):
        """
        my own implementation

        TODO - implement mulit account
        """
        latest_vals = [val for val in self.conn.accountValues() if val.tag == tag]
        if not latest_vals:
            return None
        elif len(latest_vals) == 1:
            return float(latest_vals[0].value)

        for val in latest_vals:
            if val.currency == currency:
                return float(val.value)

    def getposition(self, contract, clone=False):
        """New implementation with async"""

        position = None
        for pos in self.conn.positions():
            if contract.conId == pos.contract.conId:
                position = Position(pos.position, pos.avgCost)

        if clone:
            return copy(position)
        return position

    def nextReqId(self):
        """Mimic old nextValidId + itertools.count combo"""
        # Dont actually increment it because then reqID will be incremented AGAIN when
        # getReqId is called by IB(). It will be out of sync.
        return self.conn.client._reqIdSeq

    def cancelOrder(self, order):
        """Proxy to cancelOrder"""
        self.conn.cancelOrder(order)

    def placeOrder(self, contract, order):
        """Proxy to placeOrder"""
        self.conn.placeOrder(contract, order)

    def timeoffset(self):
        """Simplified version using async reqCurrentTime. Always returns a positive timedelta."""
        if self.p.timeoffset:
            servertime = self.conn.reqCurrentTime()
            # this doesnt seem to work below 1 sec so we ened to sleep it for 1 sec min
            self.conn.sleep(1)
            localtime = datetime.now(timezone.utc)
            return abs(localtime - servertime)
        return timedelta()

    def makecontract(self, symbol, sectype, exch, curr, expiry="", strike=0.0, right="", mult=1):
        """returns a contract from the parameters without check"""

        contract = Contract()
        contract.symbol = symbol
        contract.secType = sectype
        contract.exchange = exch
        if curr:
            contract.currency = curr
        if sectype in ["FUT", "OPT", "FOP"]:
            contract.expiry = expiry
        if sectype in ["OPT", "FOP"]:
            contract.strike = strike
            contract.right = right
        if mult:
            contract.multiplier = mult
        return contract

    def getContractDetails(self, contract, maxcount=None):
        """Get contract details without old queue and threading system"""

        cds = self.conn.reqContractDetails(contract)
        if not cds or (maxcount and len(cds) > maxcount):
            err = "Ambiguous contract: none/multiple answers received"
            self.notifs.put((err, cds, {}))
            return None
        return cds

    def reqMktData(self, contract):
        """Creates a MarketData subscription

        Params:
          - contract: a ib_async.Contract intance

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        """
        # get a ticker/queue for identification/data delivery
        ticks = "233"  # request RTVOLUME tick delivered over tickString
        reqId = self.nextReqId()

        if contract.secType in ["CASH", "CFD"]:
            ticks = ""  # cash markets do not get RTVOLUME

        ticker = self.conn.reqMktData(contract, genericTickList=ticks, snapshot=False)
        return reqId, ticker

    def cancelMktData(self, contract):
        """Cancels an existing MarketData subscription"""

        self.conn.cancelMktData(contract)

    def reqRealTimeBars(self, contract, what=None, useRTH=False):
        """Creates a request for (5 seconds) Real Time Bars"""

        if contract.secType in ["CASH", "CFD", "FUT"]:
            if not what:
                what = "BID"  # TRADES doesn't work
        else:
            what = what or "TRADES"

        reqId = self.nextReqId()
        ticker = self.conn.reqRealTimeBars(contract, barSize=5, whatToShow=what, useRTH=useRTH)
        return reqId, ticker

    def cancelRealTimeBars(self, ticker):
        """Cancels an existing MarketData subscription

        Args:
            The thing that realtimeBars returns. IB_Async just calls bars.reqID to cancel
        """
        self.conn.cancelRealTimeBars(ticker)

    def reqHistoricalData(self, contract, enddate, duration, barsize, what=None, useRTH=False):
        """Proxy to reqHistorical Data"""

        if contract.secType in ["CASH", "CFD", "FUT"]:
            if not what:
                what = "BID"  # TRADES doesn't work
        else:
            what = what or "TRADES"

        reqId = self.nextReqId()
        ticker = self.conn.reqHistoricalData(
            contract=contract,
            endDateTime=enddate,
            durationStr=duration,
            barSizeSetting=barsize,
            whatToShow=what,
            useRTH=useRTH,
            formatDate=2,
        )
        return reqId, ticker

    def getFeed(self, reqId, contract):
        return self.datas[contract.conId].feed_by_id(reqId)

    def cancelFeed(self, feed, sendnone=False):
        """Cancels a Queue for data delivery"""

        # dereference the list and unassign any subscibed datas
        feed.dereference(sendnone=sendnone)

    def getdurations(self, timeframe, compression):
        key = (timeframe, compression)
        if key not in self.revdur:
            return []

        return self.revdur[key]

    def getmaxduration(self, timeframe, compression):
        key = (timeframe, compression)
        try:
            return self.revdur[key][-1]
        except (KeyError, IndexError):
            pass

        return None

    def dt_plus_duration(self, dt, duration):
        size, dim = duration.split()
        size = int(size)
        if dim == "S":
            return dt + timedelta(seconds=size)

        if dim == "D":
            return dt + timedelta(days=size)

        if dim == "W":
            return dt + timedelta(days=size * 7)

        if dim == "M":
            month = dt.month - 1 + size  # -1 to make it 0 based, readd below
            years, month = divmod(month, 12)
            return dt.replace(year=dt.year + years, month=month + 1)

        if dim == "Y":
            return dt.replace(year=dt.year + size)

        return dt  # could do nothing with it ... return it intact

    def tfcomp_to_size(self, timeframe, compression):
        if timeframe == TimeFrame.Months:
            return "{} M".format(compression)

        if timeframe == TimeFrame.Weeks:
            return "{} W".format(compression)

        if timeframe == TimeFrame.Days:
            if not compression % 7:
                return "{} W".format(compression // 7)

            return "{} day".format(compression)

        if timeframe == TimeFrame.Minutes:
            if not compression % 60:
                hours = compression // 60
                return ("{} hour".format(hours)) + ("s" * (hours > 1))

            return ("{} min".format(compression)) + ("s" * (compression > 1))

        if timeframe == TimeFrame.Seconds:
            return "{} secs".format(compression)

        # Microseconds or ticks
        return None

    def reqHistoricalDataEx(
        self,
        contract,
        enddate,
        begindate,
        timeframe,
        compression,
        what=None,
        useRTH=False
    ):
        """
        Extension of the raw reqHistoricalData proxy, which takes two dates
        rather than a duration, barsize and date

        It uses the IB published valid duration/barsizes to make a mapping and
        spread a historical request over several historical requests if needed
        """
        # Keep a copy for error reporting purposes
        kwargs = locals().copy()
        kwargs.pop("self", None)  # remove self, no need to report it

        if timeframe < TimeFrame.Seconds:
            # Ticks are not supported
            return None, None

        if enddate is None:
            enddate = datetime.now()

        if begindate is None:
            duration = self.getmaxduration(timeframe, compression)
            if duration is None:
                err = "No duration for historical data request for " "timeframe/compresison"
                self.notifs.put((err, (), kwargs))
                return None, None
            barsize = self.tfcomp_to_size(timeframe, compression)
            if barsize is None:
                err = "No supported barsize for historical data request for " "timeframe/compresison"
                self.notifs.put((err, (), kwargs))
                return None, None

            return self.reqHistoricalData(
                contract=contract,
                enddate=enddate,
                duration=duration,
                barsize=barsize,
                what=what,
                useRTH=useRTH,
            )

        # Check if the requested timeframe/compression is supported by IB
        durations = self.getdurations(timeframe, compression)
        if not durations:  # return a queue and put a None in it
            return None, None

        # Get the best possible duration to reduce number of requests
        duration = None
        for dur in durations:
            intdate = self.dt_plus_duration(begindate, dur)
            if intdate >= enddate:
                intdate = enddate
                duration = dur  # begin -> end fits in single request
                break

        if duration is None:  # no duration large enough to fit the request
            duration = durations[-1]

        barsize = self.tfcomp_to_size(timeframe, compression)
        return self.reqHistoricalData(
            contract=contract,
            enddate=enddate,
            duration=duration,
            barsize=barsize,
            what=what,
            useRTH=useRTH,
        )

    def connectedEvent(self):
        """
        Called after successfully connecting and synchronizing with TWS/gateway.
        """
        pass

    def disconnectedEvent(self):
        """
        Called after disconnecting from TWS/gateway.
        """
        self.conn.disconnect()

    def updateEvent(self):
        """
        Called after a network packet has been handled.
        """
        pass

    def pendingTickersEvent(self, tickers):
        """
        Called with the set of tickers that have been updated during the last
        update and for which there are new ticks, tickByTicks or domTicks.
        """
        pass

    def barUpdateEvent(self, bars, hasNewBar):
        """
        Called when a bar list has been updated in real time.
        """
        pass

    def newOrderEvent(self, ibtrade):
        """
        Called when a new trade has been placed.
        - Doesnt seem to be used in backtrader
        """
        pass

    def orderModifyEvent(self, ibtrade):
        """
        Called when an order is modified.
        - Doesnt seem to be used in backtrader
        """
        pass

    def cancelOrderEvent(self, ibtrade):
        """
        Called directly after requesting for a trade to be cancelled.
        - Doesnt seem to be used in backtrader
        """
        pass

    def openOrderEvent(self, ibtrade):
        """
        Called with a trade with open order.
        """
        self.broker.push_orderstate(ibtrade)

    def orderStatusEvent(self, ibtrade):
        """
        Called when the order status of an ongoing trade changes.

        """
        self.broker.push_orderstatus(ibtrade)

    def execDetailsEvent(self, ibtrade, fill):
        """
        Called with the fill and the ongoing trade it belongs to.
        """
        self.broker.push_execution(fill.execution)
        pass

    def commissionReportEvent(self, ibtrade, fill, report):
        """
        Called with the commission report after the fill it belongs to.
        """
        self.broker.push_commissionreport(ibtrade, fill, report)

    def updatePortfolioEvent(self, item):
        """
        Called when a portfolio item has changed.
        """
        self.broker.push_portupdate()

    def positionEvent(self, position):
        """
        Called when a position has changed.
        """
        pass  # Not implemented yet

    def accountValueEvent(self, accval):
        """
        Called when an account value has changed.
        """
        # should no longer be needed due to moving away from event driven
        # Refer to inital commit for implementation
        pass

    def accountSummaryEvent(self, value):
        """
        Called when an account summary value has changed.
        """
        pass

    def pnlEvent(self, entry):
        """
        Called when a profit-and-loss entry is updated.
        """
        pass

    def pnlSingleEvent(self, entry):
        """
        Called when a profit-and-loss entry for a single position is updated.
        """
        pass

    def scannerDataEvent(self, data):
        """
        Called when data from a scanner subscription is available.
        """
        pass

    def tickNewsEvent(self, news):
        """
        Called when a new news headline is available.
        """
        pass

    def newsBulletinEvent(self, bulletin):
        """
        Called when a new news bulletin is available.
        """
        pass

    def wshMetaEvent(self, dataJson):
        """
        Called when WSH metadata is available.
        """
        pass

    def wshEvent(self, dataJson):
        """
        Called with WSH event data (earnings dates, dividend dates,
        options expiration dates, splits, spinoffs and conferences).
        """
        pass

    def errorEvent(self, reqId, errorCode, errorString, contract):
        """
        Called when an error occurs.
        """
        if not self.p.notifyall:
            msg = (reqId, errorCode, errorString, contract)
            print(f'Error occured: {msg}')
            self.notifs.put((msg, (), {}))

        # Manage those events which have to do with connection
        if errorCode is None:
            # Usually received as an error in connection of just before disconn
            pass

        elif errorCode in [200, 203, 162, 320, 321, 322]:
            # cdetails 200 security not found, notify over right queue
            # cdetails 203 security not allowed for acct
            self.cancelFeed(self.getFeed(reqId, contract), True)

        elif errorCode in [354, 420]:
            # 354 no subscription, 420 no real-time bar for contract
            # the calling data to let the data know ... it cannot resub
            feed = self.getFeed(reqId, contract)
            feed.put_error(-errorCode)
            self.cancelFeed(feed, True)

        elif errorCode == 10225:
            # 10225-Bust event occurred, current subscription is deactivated.
            # Please resubscribe real-time bars immediately.
            self.getFeed(reqId, contract).put_error(-errorCode)

        elif errorCode == 326:  # not recoverable, clientId in use
            self.dontreconnect = True
            self.conn.disconnect()
            self.stopdatas()

        elif errorCode == 502:
            # Cannot connect to TWS: port, config not open, tws off (504 then)
            self.conn.disconnect()
            self.stopdatas()

        elif errorCode == 504:  # Not Connected for data op
            # Once for each data
            pass  # don't need to manage it

        elif errorCode == 1300:
            # TWS has been closed. The port for a new connection is there
            # newport = int(msg.errorMsg.split('-')[-1])  # bla bla bla -7496
            self.conn.disconnect()
            self.stopdata()

        elif errorCode == 1100:
            # Connection lost - Notify ... datas will wait on the queue
            # with no messages arriving
            for _, data in self.datas.items():
                data.feed_by_id(reqId).put_error(-errorCode)

        elif errorCode == 1101:
            # Connection restored and tickerIds are gone
            for _, data in self.datas.items():
                data.feed_by_id(reqId).put_error(-errorCode)

        elif errorCode == 1102:
            # Connection restored and tickerIds maintained
            for _, data in self.datas.items():
                data.feed_by_id(reqId).put_error(-errorCode)

        elif errorCode < 500:
            # Given the myriad of errorCodes, start by assuming is an order
            # error and if not, the checks there will let it go
            if reqId < self.REQIDBASE:
                if self.broker is not None:
                    self.broker.push_ordererror(reqId, errorCode)
            else:
                # Cancel the queue if a "data" reqId error is given: sanity
                self.cancelFeed(self.getFeed(reqId, contract), True)

    def timeoutEvent(self, idlePeriod):
        """
        Called if no data is received for longer than the timeout period.
        """
        pass
