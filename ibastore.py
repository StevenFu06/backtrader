import collections
from copy import copy
import random

from ib_async import IB
from backtrader import Position
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
        ("refreshrate", 0.05),  # IB_Async now usses ib.sleep to refresh its values
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
        self.broker = None  # broker instance

        self.acc_cash = AutoDict()  # current total cash per account
        self.acc_value = AutoDict()  # current total value per account
        self.acc_upds = AutoDict()  # current account valueinfos per account

        self.positions = collections.defaultdict(Position)  # actual positions

        # Create connection object
        self.conn = IB().connect(host=self.p.host, port=self.p.port, clientId=self.p.clientId)
        # Register them to async
        for event in self.events:
            getattr(self.conn, event).connect(getattr(self, event))

        # Use the provided clientId or a random one
        if self.p.clientId is None:
            self.clientId = random.randint(1, pow(2, 16) - 1)
        else:
            self.clientId = self.p.clientId

    def start(self, data=None, broker=None):
        if broker is not None:
            self.broker = broker

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

    def stop(self):
        try:
            self.conn.disconnect()  # disconnect should be an invariant
        except AttributeError:
            pass  # conn may have never been connected and lack "disconnect"

    def logmsg(self, *args):
        # for logging purposes
        if self.p._debug:
            print(*args)

    def watcher(self, msg):
        # will be registered to see all messages if debug is requested
        self.logmsg(str(msg))
        if self.p.notifyall:
            self.notifs.put((msg, tuple(msg.values()), dict(msg.items())))

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

    def reqAccountUpdates(self, subscribe=True, account=None):
        """Proxy to reqAccountUpdates

        If ``account`` is ``None``, wait for the ``managedAccounts`` message to
        set the account codes
        """
        if account is None:
            self._event_managed_accounts.wait()
            account = self.managed_accounts[0]

        self.conn.reqAccountUpdates(subscribe, bytes(account))

    def get_acc_value(self, tag):
        """
        my own implementation

        TODO - implement mulit account
        """
        latest_vals = [val for val in self.conn.accountValues() if val.tag == tag]
        if not latest_vals:
            return None

        for val in latest_vals:
            if val.currency == "USD":
                return float(val.value)

    def getposition(self, dataname, clone=False):
        """New implementation with async"""

        position = None
        for pos in self.conn.positions():
            if dataname == pos.contract.symbol:
                position = Position(pos.position, pos.avgCost)

        if clone:
            return copy(position)
        return position

    def nextOrderId(self):
        """Mimic old nextValidId + itertools.count combo"""
        return self.conn.client.getReqId()

    def cancelOrder(self, order):
        """Proxy to cancelOrder"""
        self.conn.cancelOrder(order)

    def placeOrder(self, contract, order):
        """Proxy to placeOrder"""
        self.conn.placeOrder(contract, order)

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
        try:
            value = float(accval.value)
        except ValueError:
            value = accval.value
        self.acc_upds[accval.account][accval.tag][accval.currency] = value

        if accval.tag == "NetLiquidation":
            # NetLiquidationByCurrency and currency == 'BASE' is the same
            self.acc_value[accval.account] = value
        elif accval.tag == "TotalCashBalance" and accval.currency == "BASE":
            self.acc_cash[accval.account] = value

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
