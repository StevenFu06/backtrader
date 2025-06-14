#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2023 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import absolute_import, division, print_function, unicode_literals

import collections
from copy import copy
from datetime import date, datetime, timedelta
import threading
import uuid

import ib_async.order

from backtrader.feed import DataBase
from backtrader import TimeFrame, num2date, date2num, BrokerBase, Order, OrderBase, OrderData
from backtrader.utils.py3 import bytes, bstr, with_metaclass, queue, MAXFLOAT
from backtrader.metabase import MetaParams
from backtrader.comminfo import CommInfoBase
from backtrader.position import Position
from backtrader.utils import AutoDict, AutoOrderedDict
from backtrader.comminfo import CommInfoBase

from backtrader.stores.ibastore import IBAStore

bytes = bstr  # py2/3 need for ibpy


# class IBOrderState(object):
#     # wraps OrderState object and can print it
#     _fields = ['status', 'initMargin', 'maintMargin', 'equityWithLoan',
#                'commission', 'minCommission', 'maxCommission',
#                'commissionCurrency', 'warningText']

#     def __init__(self, orderstate):
#         for f in self._fields:
#             fname = 'm_' + f
#             setattr(self, fname, getattr(orderstate, fname))

#     def __str__(self):
#         txt = list()
#         txt.append('--- ORDERSTATE BEGIN')
#         for f in self._fields:
#             fname = 'm_' + f
#             txt.append('{}: {}'.format(f.capitalize(), getattr(self, fname)))
#         txt.append('--- ORDERSTATE END')
#         return '\n'.join(txt)


class IBAOrder(OrderBase, ib_async.order.Order):
    """Subclasses the IBPy order to provide the minimum extra functionality
    needed to be compatible with the internally defined orders

    Once ``OrderBase`` has processed the parameters, the __init__ method takes
    over to use the parameter values and set the appropriate values in the
    ib.ext.Order.Order object

    Any extra parameters supplied with kwargs are applied directly to the
    ib.ext.Order.Order object, which could be used as follows::

      Example: if the 4 order execution types directly supported by
      ``backtrader`` are not enough, in the case of for example
      *Interactive Brokers* the following could be passed as *kwargs*::

        orderType='LIT', lmtPrice=10.0, auxPrice=9.8

      This would override the settings created by ``backtrader`` and
      generate a ``LIMIT IF TOUCHED`` order with a *touched* price of 9.8
      and a *limit* price of 10.0.

    This would be done almost always from the ``Buy`` and ``Sell`` methods of
    the ``Strategy`` subclass being used in ``Cerebro``
    """

    def __str__(self):
        """Get the printout from the base class and add some ib.Order specific
        fields"""
        basetxt = super(IBAOrder, self).__str__()
        tojoin = [basetxt]
        tojoin.append("Ref: {}".format(self.ref))
        tojoin.append("orderId: {}".format(self.m_orderId))
        tojoin.append("Action: {}".format(self.m_action))
        tojoin.append("Size (ib): {}".format(self.m_totalQuantity))
        tojoin.append("Lmt Price: {}".format(self.m_lmtPrice))
        tojoin.append("Aux Price: {}".format(self.m_auxPrice))
        tojoin.append("OrderType: {}".format(self.m_orderType))
        tojoin.append("Tif (Time in Force): {}".format(self.m_tif))
        tojoin.append("GoodTillDate: {}".format(self.m_goodTillDate))
        return "\n".join(tojoin)

    # Map backtrader order types to the ib specifics
    _IBOrdTypes = {
        None: bytes("MKT"),  # default
        Order.Market: bytes("MKT"),
        Order.Limit: bytes("LMT"),
        Order.Close: bytes("MOC"),
        Order.Stop: bytes("STP"),
        Order.StopLimit: bytes("STPLMT"),
        Order.StopTrail: bytes("TRAIL"),
        Order.StopTrailLimit: bytes("TRAIL LIMIT"),
    }

    def __init__(self, action, **kwargs):

        # Marker to indicate an openOrder has been seen with
        # PendingCancel/Cancelled which is indication of an upcoming
        # cancellation
        self._willexpire = False

        self.ordtype = self.Buy if action == "BUY" else self.Sell

        super(IBAOrder, self).__init__()
        ib_async.order.Order.__init__(self)  # Invoke 2nd base class

        # Now fill in the specific IB parameters
        self.orderType = self._IBOrdTypes[self.exectype]
        self.permid = 0

        # 'B' or 'S' should be enough
        self.action = bytes(action)

        # Set the prices
        self.lmtPrice = 0.0
        self.auxPrice = 0.0

        if self.exectype == self.Market:
            pass
        elif self.exectype == self.Close:
            pass
        elif self.exectype == self.Limit:
            self.lmtPrice = self.price
        elif self.exectype == self.Stop:
            self.auxPrice = self.price  # stop price / exec is market
        elif self.exectype == self.StopLimit:
            self.lmtPrice = self.pricelimit  # req limit execution
            self.auxPrice = self.price  # trigger price
        elif self.exectype == self.StopTrail:
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.trailingPercent = self.trailpercent * 100.0
        elif self.exectype == self.StopTrailLimit:
            self.trailStopPrice = self.lmtPrice = self.price
            # The limit offset is set relative to the price difference in TWS
            self.lmtPrice = self.pricelimit
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.trailingPercent = self.trailpercent * 100.0

        self.totalQuantity = abs(self.size)  # ib takes only positives

        self.transmit = self.transmit
        if self.parent is not None:
            self.parentId = self.parent.m_orderId

        # Time In Force: DAY, GTC, IOC, GTD
        if self.valid is None:
            tif = "GTC"  # Good til cancelled
        elif isinstance(self.valid, (datetime, date)):
            tif = "GTD"  # Good til date
            self.goodTillDate = bytes(self.valid.strftime("%Y%m%d %H:%M:%S"))
        elif isinstance(self.valid, (timedelta,)):
            if self.valid == self.DAY:
                tif = "DAY"
            else:
                tif = "GTD"  # Good til date
                valid = datetime.now() + self.valid  # .now, using localtime
                self.goodTillDate = bytes(valid.strftime("%Y%m%d %H:%M:%S"))

        elif self.valid == 0:
            tif = "DAY"
        else:
            tif = "GTD"  # Good til date
            valid = num2date(self.valid)
            self.goodTillDate = bytes(valid.strftime("%Y%m%d %H:%M:%S"))

        self.tif = bytes(tif)

        # OCA
        self.ocaType = 1  # Cancel all remaining orders with block

        # pass any custom arguments to the order
        for k in kwargs:
            setattr(self, (not hasattr(self, k)) * "m_" + k, kwargs[k])


class IBACommInfo(CommInfoBase):
    """
    Commissions are calculated by ib, but the trades calculations in the
    ```Strategy`` rely on the order carrying a CommInfo object attached for the
    calculation of the operation cost and value.

    These are non-critical informations, but removing them from the trade could
    break existing usage and it is better to provide a CommInfo objet which
    enables those calculations even if with approvimate values.

    The margin calculation is not a known in advance information with IB
    (margin impact can be gotten from OrderState objects) and therefore it is
    left as future exercise to get it"""

    def getvaluesize(self, size, price):
        # In real life the margin approaches the price
        return abs(size) * price

    def getoperationcost(self, size, price):
        """Returns the needed amount of cash an operation would cost"""
        # Same reasoning as above
        return abs(size) * price


class MetaIBABroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        """Class has already been created ... register"""
        # Initialize the class
        super(MetaIBABroker, cls).__init__(name, bases, dct)
        IBAStore.BrokerCls = cls


class IBABroker(with_metaclass(MetaIBABroker, BrokerBase)):
    """Broker implementation for Interactive Brokers.

    This class maps the orders/positions from Interactive Brokers to the
    internal API of ``backtrader``.

    Notes:

      - ``tradeid`` is not really supported, because the profit and loss are
        taken directly from IB. Because (as expected) calculates it in FIFO
        manner, the pnl is not accurate for the tradeid.

      - Position

        If there is an open position for an asset at the beginning of
        operaitons or orders given by other means change a position, the trades
        calculated in the ``Strategy`` in cerebro will not reflect the reality.

        To avoid this, this broker would have to do its own position
        management which would also allow tradeid with multiple ids (profit and
        loss would also be calculated locally), but could be considered to be
        defeating the purpose of working with a live broker
    """

    params = (
        # If theres multiple values this is the default currency it returns
        ("currency", "BASE"),
    )

    def __init__(self, **kwargs):
        super(IBABroker, self).__init__()

        self.ib = IBAStore(**kwargs)
        self.do_refresh = False

        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0

        self.orderbyid = dict()  # orders by order id
        self.executions = dict()  # notified executions
        self.ordstatus = collections.defaultdict(dict)
        self.notifs = queue.Queue()  # holds orders which are notified
        self.tonotify = collections.deque()  # hold oids to be notified

    def start(self):
        super(IBABroker, self).start()
        self.ib.start(broker=self)

        if not self.ib.refreshing:
            self.do_refresh = True
            self.ib.refreshing = True

        if self.ib.connected():
            self.startingcash = self.cash = self.ib.getAccountValues("TotalCashBalance")
            self.startingvalue = self.value = self.ib.getAccountValues("NetLiquidation")
        else:
            self.startingcash = self.cash = 0.0
            self.startingvalue = self.value = 0.0

    def stop(self):
        super(IBABroker, self).stop()
        self.ib.stop()

    def next(self):
        self.notifs.put(None)  # mark notification boundary
        if self.do_refresh:  # refresh ib_async
            self.ib.conn.sleep(self.ib.p.refreshrate)

    def getcash(self):
        self.cash = self.ib.getAccountValues("TotalCashBalance", self.p.currency)
        return self.cash

    def getvalue(self, datas=None):
        self.value = self.ib.getAccountValues("NetLiquidation", self.p.currency)
        return self.value

    def getposition(self, data, clone=True):
        return self.ib.getposition(data.tradecontract, clone=clone)
    
    def getportfolio(self, data, clone=True):
        return self.ib.getAssetPortfolio(data.tradecontract, clone=True)

    def cancel(self, order):
        try:
            o = self.orderbyid[order.orderId]
        except (ValueError, KeyError):
            return  # not found ... not cancellable

        if order.status == Order.Cancelled:  # already cancelled
            return

        self.ib.cancelOrder(order)

    def orderstatus(self, order):
        try:
            o = self.orderbyid[order.orderId]
        except (ValueError, KeyError):
            o = order

        return o.status

    def submit(self, order):
        order.submit(self)

        # ocoize if needed
        if order.oco is None:  # Generate a UniqueId
            order.ocaGroup = bytes(uuid.uuid4())
        else:
            order.ocaGroup = self.orderbyid[order.oco.orderId].ocaGroup

        self.orderbyid[order.orderId] = order
        self.ib.placeOrder(order.data.tradecontract, order)
        self.notify(order)

        return order

    def getcommissioninfo(self, data):
        contract = data.tradecontract
        try:
            mult = float(contract.multiplier)
        except (ValueError, TypeError):
            mult = 1.0

        stocklike = contract.secType not in (
            "FUT",
            "OPT",
            "FOP",
        )

        return IBACommInfo(mult=mult, stocklike=stocklike)

    def _makeorder(
        self, action, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, **kwargs
    ):
        oid = self.ib.nextReqId()
        order = IBAOrder(
            action,
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            clientId=self.ib.clientId,
            orderId=oid,
            **kwargs
        )

        order.addcomminfo(self.getcommissioninfo(data))
        return order

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, **kwargs):

        order = self._makeorder("BUY", owner, data, size, price, plimit, exectype, valid, tradeid, **kwargs)

        return self.submit(order)

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, **kwargs):

        order = self._makeorder("SELL", owner, data, size, price, plimit, exectype, valid, tradeid, **kwargs)

        return self.submit(order)

    def notify(self, order):
        self.notifs.put(order.clone())

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            pass

        return None

    # Order statuses in msg
    (SUBMITTED, FILLED, CANCELLED, INACTIVE, PENDINGSUBMIT, PENDINGCANCEL, PRESUBMITTED) = (
        "Submitted",
        "Filled",
        "Cancelled",
        "Inactive",
        "PendingSubmit",
        "PendingCancel",
        "PreSubmitted",
    )

    def push_orderstatus(self, ibtrade):
        # Cancelled and Submitted with Filled = 0 can be pushed immediately
        try:
            order = self.orderbyid[ibtrade.order.orderId]
        except KeyError:
            return  # not found, it was not an order

        trade_status = ibtrade.orderStatus.status

        if trade_status == self.SUBMITTED and ibtrade.filled() == 0:
            if order.status == order.Accepted:  # duplicate detection
                return

            order.accept(self)
            self.notify(order)

        elif trade_status == self.CANCELLED:
            # duplicate detection
            if order.status in [order.Cancelled, order.Expired]:
                return

            if order._willexpire:
                # An openOrder has been seen with PendingCancel/Cancelled
                # and this happens when an order expires
                order.expire()
            else:
                # Pure user cancellation happens without an openOrder
                order.cancel()
            self.notify(order)

        elif trade_status == self.PENDINGCANCEL:
            # In theory this message should not be seen according to the docs,
            # but other messages like PENDINGSUBMIT which are similarly
            # described in the docs have been received in the demo
            if order.status == order.Cancelled:  # duplicate detection
                return

            # We do nothing because the situation is handled with the 202 error
            # code if no orderStatus with CANCELLED is seen
            # order.cancel()
            # self.notify(order)

        elif trade_status == self.INACTIVE:
            # This is a tricky one, because the instances seen have led to
            # order rejection in the demo, but according to the docs there may
            # be a number of reasons and it seems like it could be reactivated
            if order.status == order.Rejected:  # duplicate detection
                return

            order.reject(self)
            self.notify(order)

        # elif trade_status in [self.SUBMITTED, self.FILLED]:
        #     # These two are kept inside the order until execdetails and
        #     # commission are all in place - commission is the last to come
        #     self.ordstatus[ibtrade.order.orderId][ibtrade.filled()] = ibtrade

        # elif trade_status in [self.PENDINGSUBMIT, self.PRESUBMITTED]:
        #     # According to the docs, these statuses can only be set by the
        #     # programmer but the demo account sent it back at random times with
        #     # "filled"
        #     if ibtrade.filled():
        #         self.ordstatus[ibtrade.order.orderId][ibtrade.filled()] = ibtrade
        else:  # Unknown status ...
            pass

    def push_execution(self, execution):
        self.executions[execution.execId] = execution

    def push_commissionreport(self, ibtrade, fill, report):
        ex = fill.execution
        oid = ex.orderId
        order = self.orderbyid[oid]

        position = self.getposition(order.data, clone=False)
        pprice_orig = position.price

        size = ex.shares if ex.side[0] == "B" else -ex.shares
        price = ex.price
        # use pseudoupdate and let the updateportfolio do the real update?
        psize, pprice, opened, closed = position.update(size, price)

        # split commission between closed and opened
        comm = report.commission
        closedcomm = comm * closed / size
        openedcomm = comm - closedcomm

        comminfo = order.comminfo
        closedvalue = comminfo.getoperationcost(closed, pprice_orig)
        openedvalue = comminfo.getoperationcost(opened, price)

        # default in m_pnl is MAXFLOAT
        pnl = report.realizedPNL if closed else 0.0

        # The internal broker calc should yield the same result
        # pnl = comminfo.profitandloss(-closed, pprice_orig, price)

        # Use the actual time provided by the execution object
        # The report from TWS is in actual local time, not the data's tz
        dt = date2num(ex.time)

        # Need to simulate a margin, but it plays no role, because it is
        # controlled by a real broker. Let's set the price of the item
        margin = order.data.close[0]

        order.execute(
            dt,
            size,
            price,
            closed,
            closedvalue,
            closedcomm,
            opened,
            openedvalue,
            openedcomm,
            margin,
            pnl,
            psize,
            pprice,
        )

        if ibtrade.orderStatus.status == self.FILLED:
            order.completed()
        else:
            order.partial()

        if oid not in self.tonotify:  # Lock needed
            self.tonotify.append(oid)

    def push_portupdate(self):
        # If the IBStore receives a Portfolio update, then this method will be
        # indicated. If the execution of an order is split in serveral lots,
        # updatePortfolio messages will be intermixed, which is used as a
        # signal to indicate that the strategy can be notified
        while self.tonotify:
            oid = self.tonotify.popleft()
            order = self.orderbyid[oid]
            self.notify(order)

    def push_ordererror(self, reqId, errorCode):
        try:
            order = self.orderbyid[reqId]
        except (KeyError, AttributeError):
            return  # no order or no id in error

        if errorCode == 202:
            if not order.alive():
                return
            order.cancel()

        elif errorCode == 201:  # rejected
            if order.status == order.Rejected:
                return
            order.reject()

        else:
            order.reject()  # default for all other cases

        self.notify(order)

    def push_orderstate(self, ibtrade):

        try:
            order = self.orderbyid[ibtrade.order.orderId]
        except (KeyError, AttributeError):
            return  # no order or no id in error

        if ibtrade.orderStatus.status in ["PendingCancel", "Cancelled", "Canceled"]:
            # This is most likely due to an expiration]
            order._willexpire = True
