import collections
from copy import copy
import random
from datetime import datetime

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
        self.datas = list()  # datas that have registered over start
        self._env = None  # reference to cerebro for general notifications
        self.iscash = dict()

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

    def start(self, data=None, broker=None):
        self.reconnect(fromstart=True)  # reconnect should be an invariant
        if data is not None:
            self._env = data._env
            self.datas.append(data)

        if broker is not None:
            self.broker = broker

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
        for data in self.datas:
            data.reqdata()
        # Force an update
        self.conn.sleep(self.p.refreshrate)

    def stopdatas(self):
        for data in self.datas:
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

    def nextOrderId(self):
        """Mimic old nextValidId + itertools.count combo"""
        # Dont actually increment it because then reqID will be incremented AGAIN when
        # getReqId is called by IB(). It will be out of sync.
        return self.conn.client._reqIdSeq + 1

    def cancelOrder(self, order):
        """Proxy to cancelOrder"""
        self.conn.cancelOrder(order)

    def placeOrder(self, contract, order):
        """Proxy to placeOrder"""
        self.conn.placeOrder(contract, order)

    def timeoffset(self):
        """Simplified version using async reqCurrentTime"""
        return self.conn.reqCurrentTime() - datetime.now()

    def makecontract(self, symbol, sectype, exch, curr, expiry="", strike=0.0, right="", mult=1):
        """returns a contract from the parameters without check"""

        contract = Contract()
        contract.symbol = bytes(symbol)
        contract.secType = bytes(sectype)
        contract.exchange = bytes(exch)
        if curr:
            contract.currency = bytes(curr)
        if sectype in ["FUT", "OPT", "FOP"]:
            contract.expiry = bytes(expiry)
        if sectype in ["OPT", "FOP"]:
            contract.strike = strike
            contract.right = bytes(right)
        if mult:
            contract.multiplier = bytes(mult)
        return contract

    def getContractDetails(self, contract, maxcount=None):
        """Get contract details without old queue and threading system"""

        cds = self.conn.reqContractDetails(contract)
        if not cds or (maxcount and len(cds) > maxcount):
            err = "Ambiguous contract: none/multiple answers received"
            self.notifs.put((err, cds, {}))
            return None
        return cds

    def reqMktData(self, contract, what=None):
        """Creates a MarketData subscription

        Params:
          - contract: a ib_async.Contract intance

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        """
        # get a ticker/queue for identification/data delivery
        ticks = "233"  # request RTVOLUME tick delivered over tickString
        ticker = self.conn.reqMktData(contract, genericTickList=ticks, snapshot=False)

        if contract.secType in ["CASH", "CFD"]:
            self.iscash[contract.conId] = True
            ticks = ""  # cash markets do not get RTVOLUME
            if what == "ASK":
                self.iscash[contract.conId] = 2

        return ticker

    def cancelMktData(self, contract):
        """Cancels an existing MarketData subscription"""

        self.conn.cancelMktData(contract)

    def reqRealTimeBars(self, contract, what="TRADES", useRTH=False):
        """Creates a request for (5 seconds) Real Time Bars"""

        ticker = self.conn.reqRealTimeBars(contract, barsize=5, whatToShow=what, useRTH=useRTH)
        return ticker
    
    def cancelRealTimeBars(self, ticker):
        '''Cancels an existing MarketData subscription
        
        Args:
            The thing that realtimeBars returns. IB_Async just calls bars.reqID to cancel
        '''
        self.conn.cancelRealTimeBars(ticker)


    def reqHistoricalData(self, contract, enddate, duration, barsize, what=None, useRTH=False, tz="", sessionend=None):
        """Proxy to reqHistorical Data"""

        # get a ticker/queue for identification/data delivery
        ticker = self.conn.reqHistoricalData(
            contract=contract,
            enddate=enddate.strftime("%Y%m%d %H:%M:%S") + " GMT",
            duration=duration,
            barsize=barsize,
            whatToShow=what,
            useRTH=useRTH,
            formatDate=2,
        )

        if contract.secType in ["CASH", "CFD"]:
            self.iscash[contract.conId] = True
            if not what:
                what = "BID"  # TRADES doesn't work
            elif what == "ASK":
                self.iscash[contract.conId] = 2
        else:
            what = what or "TRADES"

        # split barsize "x time", look in sizes for (tf, comp) get tf
        tframe = self._sizes[barsize.split()[1]][0]
        self.histfmt[contract.conId] = tframe >= TimeFrame.Days
        self.histsend[contract.conId] = sessionend
        self.histtz[contract.conId] = tz

        return ticker
    
    def cancelQueue(self, q, sendnone=False):
        '''Cancels a Queue for data delivery'''
        # pop ts (tickers) and with the result qs (queues)
        tickerId = self.ts.pop(q, None)
        self.qs.pop(tickerId, None)

        self.iscash.pop(tickerId, None)

        if sendnone:
            q.put(None)

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
        # self.broker.push_execution(fill.execution)
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
            self.notifs.put((reqId, errorCode, errorString, contract))

        # Manage those events which have to do with connection
        if errorCode is None:
            # Usually received as an error in connection of just before disconn
            pass

        elif errorCode in [200, 203, 162, 320, 321, 322]:
            # cdetails 200 security not found, notify over right queue
            # cdetails 203 security not allowed for acct
            pass

        elif errorCode in [354, 420]:
            # 354 no subscription, 420 no real-time bar for contract
            # the calling data to let the data know ... it cannot resub
            pass

        elif errorCode == 10225:
            # 10225-Bust event occurred, current subscription is deactivated.
            # Please resubscribe real-time bars immediately.
            pass

        elif errorCode == 326:  # not recoverable, clientId in use
            self.dontreconnect = True
            self.conn.disconnect()

        elif errorCode == 502:
            # Cannot connect to TWS: port, config not open, tws off (504 then)
            self.conn.disconnect()

        elif errorCode == 504:  # Not Connected for data op
            # Once for each data
            pass  # don't need to manage it

        elif errorCode == 1300:
            # TWS has been closed. The port for a new connection is there
            # newport = int(msg.errorMsg.split('-')[-1])  # bla bla bla -7496
            self.conn.disconnect()

        elif errorCode == 1100:
            # Connection lost - Notify ... datas will wait on the queue
            # with no messages arriving
            pass

        elif errorCode == 1101:
            # Connection restored and tickerIds are gone
            pass

        elif errorCode == 1102:
            # Connection restored and tickerIds maintained
            pass

        elif errorCode < 500:
            # Given the myriad of errorCodes, start by assuming is an order
            # error and if not, the checks there will let it go
            if reqId < self.REQIDBASE:
                if self.broker is not None:
                    self.broker.push_ordererror(reqId, errorCode)
            # else:
            #     # Cancel the queue if a "data" reqId error is given: sanity
            #     q = self.qs[reqId.id]
            #     self.cancelQueue(q, True)

    def timeoutEvent(self, idlePeriod):
        """
        Called if no data is received for longer than the timeout period.
        """
        pass
