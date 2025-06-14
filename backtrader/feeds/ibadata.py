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

import datetime
from typing import Union

import backtrader as bt
from backtrader.feed import DataBase
from backtrader import TimeFrame, date2num, num2date
from backtrader.utils.py3 import integer_types, queue, string_types, with_metaclass
from backtrader.metabase import MetaParams
import backtrader.stores.ibastore as ibastore
from ib_async import Ticker


class Feed:

    def __init__(self, hist=False):
        self.reqId = None
        self.data = []
        self.errors = []
        self.hist = hist
        # Used to track if the rtvolume has updated since last pop
        self._last_datetime = None

    def __bool__(self):
        return self.data is not None

    def __len__(self):
        totlen = 0
        try:
            totlen += len(self.data)
        except TypeError:
            pass
        try:
            totlen += len(self.errors)
        except TypeError:
            pass
        return totlen

    def set_data(self, reqId, feed):
        self.reqId = reqId
        self.data = feed

    def pop_data(self):
        """Note this will not pop if its realtime data (a Ticker)"""
        if self.hist:
            if len(self.data) >= 1:
                return self.data.pop(0)
            else:
                return False
        else:
            try:
                return self.data.pop(0)
            except IndexError:
                return False
            except AttributeError:
                if self._last_datetime != self.data.time:
                    self._last_datetime = self.data.time
                    return self.data
                else:
                    return False

    def put_error(self, errorcode):
        self.errors.append(errorcode)

    def pop_error(self):
        try:
            return self.errors.pop(0)
        except IndexError:
            raise IndexError("Errors queue is empty.")

    def dereference(self, sendnone=False):
        self.reqId = None
        if isinstance(self.data, Ticker):
            # if it is a ticker, Im okay with losing a single data bar
            self.data = None
        else:
            # Dereference but keep the data (until reassingment) incase theres a huge backlog
            self.data = list(self.data)
        if sendnone:
            self.errors.append(None)


class MetaIBData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        """Class has already been created ... register"""
        # Initialize the class
        super(MetaIBData, cls).__init__(name, bases, dct)

        # Register with the store
        ibastore.IBAStore.DataCls = cls


class IBAData(with_metaclass(MetaIBData, DataBase)):
    """Interactive Brokers Data Feed.

    Supports the following contract specifications in parameter ``dataname``:

          - TICKER  # Stock type and SMART exchange
          - TICKER-STK  # Stock and SMART exchange
          - TICKER-STK-EXCHANGE  # Stock
          - TICKER-STK-EXCHANGE-CURRENCY  # Stock

          - TICKER-CFD  # CFD and SMART exchange
          - TICKER-CFD-EXCHANGE  # CFD
          - TICKER-CDF-EXCHANGE-CURRENCY  # Stock

          - TICKER-IND-EXCHANGE  # Index
          - TICKER-IND-EXCHANGE-CURRENCY  # Index

          - TICKER-YYYYMM-EXCHANGE  # Future
          - TICKER-YYYYMM-EXCHANGE-CURRENCY  # Future
          - TICKER-YYYYMM-EXCHANGE-CURRENCY-MULT  # Future
          - TICKER-FUT-EXCHANGE-CURRENCY-YYYYMM-MULT # Future

          - TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT  # FOP
          - TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # FOP
          - TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT # FOP
          - TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT-MULT # FOP

          - CUR1.CUR2-CASH-IDEALPRO  # Forex

          - TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT  # OPT
          - TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # OPT
          - TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT # OPT
          - TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT-MULT # OPT

    Params:

      - ``sectype`` (default: ``STK``)

        Default value to apply as *security type* if not provided in the
        ``dataname`` specification

      - ``exchange`` (default: ``SMART``)

        Default value to apply as *exchange* if not provided in the
        ``dataname`` specification

      - ``currency`` (default: ``''``)

        Default value to apply as *currency* if not provided in the
        ``dataname`` specification

      - ``historical`` (default: ``False``)

        If set to ``True`` the data feed will stop after doing the first
        download of data.

        The standard data feed parameters ``fromdate`` and ``todate`` will be
        used as reference.

        The data feed will make multiple requests if the requested duration is
        larger than the one allowed by IB given the timeframe/compression
        chosen for the data.

      - ``what`` (default: ``None``)

        If ``None`` the default for different assets types will be used for
        historical data requests:

          - 'BID' for CASH assets
          - 'TRADES' for any other

        Use 'ASK' for the Ask quote of cash assets

        Check the IB API docs if another value is wished

      - ``rtbar`` (default: ``False``)

        If ``True`` the ``5 Seconds Realtime bars`` provided by Interactive
        Brokers will be used as the smalles tick. According to the
        documentation they correspond to real-time values (once collated and
        curated by IB)

        If ``False`` then the ``RTVolume`` prices will be used, which are based
        on receiving ticks. In the case of ``CASH`` assets (like for example
        EUR.JPY) ``RTVolume`` will always be used and from it the ``bid`` price
        (industry de-facto standard with IB according to the literature
        scattered over the Internet)

        Even if set to ``True``, if the data is resampled/kept to a
        timeframe/compression below Seconds/5, no real time bars will be used,
        because IB doesn't serve them below that level

      - ``qcheck`` (default: ``0.5``)

        Time in seconds to wake up if no data is received to give a chance to
        resample/replay packets properly and pass notifications up the chain

      - ``backfill_start`` (default: ``True``)

        Perform backfilling at the start. The maximum possible historical data
        will be fetched in a single request.

      - ``backfill`` (default: ``True``)

        Perform backfilling after a disconnection/reconnection cycle. The gap
        duration will be used to download the smallest possible amount of data

      - ``backfill_from`` (default: ``None``)

        An additional data source can be passed to do an initial layer of
        backfilling. Once the data source is depleted and if requested,
        backfilling from IB will take place. This is ideally meant to backfill
        from already stored sources like a file on disk, but not limited to.

      - ``latethrough`` (default: ``False``)

        If the data source is resampled/replayed, some ticks may come in too
        late for the already delivered resampled/replayed bar. If this is
        ``True`` those ticks will bet let through in any case.

        Check the Resampler documentation to see who to take those ticks into
        account.

        This can happen especially if ``timeoffset`` is set to ``False``  in
        the ``IBStore`` instance and the TWS server time is not in sync with
        that of the local computer

      - ``tradename`` (default: ``None``)
        Useful for some specific cases like ``CFD`` in which prices are offered
        by one asset and trading happens in a different onel

        - SPY-STK-SMART-USD -> SP500 ETF (will be specified as ``dataname``)

        - SPY-CFD-SMART-USD -> which is the corresponding CFD which offers not
          price tracking but in this case will be the trading asset (specified
          as ``tradename``)

    The default values in the params are the to allow things like ```TICKER``,
    to which the parameter ``sectype`` (default: ``STK``) and ``exchange``
    (default: ``SMART``) are applied.

    Some assets like ``AAPL`` need full specification including ``currency``
    (default: '') whereas others like ``TWTR`` can be simply passed as it is.

      - ``AAPL-STK-SMART-USD`` would be the full specification for dataname

        Or else: ``IBAData`` as ``IBAData(dataname='AAPL', currency='USD')``
        which uses the default values (``STK`` and ``SMART``) and overrides
        the currency to be ``USD``
    """

    params = (
        ("sectype", "STK"),  # usual industry value
        ("exchange", "SMART"),  # usual industry value
        ("currency", ""),
        ("rtbar", False),  # use RealTime 5 seconds bars
        ("historical", False),  # only historical download
        ("what", None),  # historical - what to show
        ("useRTH", False),  # historical - download only Regular Trading Hours
        ("qcheck", 0.5),  # timeout in seconds (float) to check for events
        ("backfill_start", True),  # do backfilling at the start
        ("backfill", True),  # do backfilling when reconnecting
        ("backfill_from", None),  # additional data source to do backfill from
        ("latethrough", False),  # let late samples through
        ("tradename", None),  # use a different asset as order target
    )

    _store = ibastore.IBAStore

    # Minimum size supported by real-time bars
    RTBAR_MINSIZE = (TimeFrame.Seconds, 5)

    # States for the Finite State Machine in _load
    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)

    def _timeoffset(self):
        # This is needed for the resampler
        offset = self.ib.timeoffset()
        return offset

    def _gettz(self):
        # If no object has been provided by the user and a timezone can be
        # found via contractdtails, then try to get it from pytz, which may or
        # may not be available.

        # The timezone specifications returned by TWS seem to be abbreviations
        # understood by pytz, but the full list which TWS may return is not
        # documented and one of the abbreviations may fail
        tzstr = isinstance(self.p.tz, string_types)
        if self.p.tz is not None and not tzstr:
            return bt.utils.date.Localizer(self.p.tz)

        if self.contractdetails is None:
            return None  # nothing can be done

        try:
            import pytz  # keep the import very local
        except ImportError:
            return None  # nothing can be done

        tzs = self.p.tz if tzstr else self.contractdetails.timeZoneId

        if tzs == "CST":  # reported by TWS, not compatible with pytz. patch it
            tzs = "CST6CDT"

        try:
            tz = pytz.timezone(tzs)
        except pytz.UnknownTimeZoneError:
            return None  # nothing can be done

        # contractdetails there, import ok, timezone found, return it
        return tz

    def islive(self):
        """Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated"""
        return not self.p.historical

    def __init__(self, **kwargs):
        self.ib = self._store(**kwargs)
        self.do_refresh = False

        self.precontract = self.parsecontract(self.p.dataname)
        self.pretradecontract = self.parsecontract(self.p.tradename)

    def setenvironment(self, env):
        """Receives an environment (cerebro) and passes it over to the store it
        belongs to"""
        super(IBAData, self).setenvironment(env)
        env.addstore(self.ib)

    def parsecontract(self, dataname):
        """Parses dataname generates a default contract"""
        # Set defaults for optional tokens in the ticker string
        if dataname is None:
            return None

        exch = self.p.exchange
        curr = self.p.currency
        expiry = ""
        strike = 0.0
        right = ""
        mult = ""

        # split the ticker string
        tokens = iter(dataname.split("-"))

        # Symbol and security type are compulsory
        symbol = next(tokens)
        try:
            sectype = next(tokens)
        except StopIteration:
            sectype = self.p.sectype

        # security type can be an expiration date
        if sectype.isdigit():
            expiry = sectype  # save the expiration ate

            if len(sectype) == 6:  # YYYYMM
                sectype = "FUT"
            else:  # Assume OPTIONS - YYYYMMDD
                sectype = "OPT"

        if sectype == "CASH":  # need to address currency for Forex
            symbol, curr = symbol.split(".")

        # See if the optional tokens were provided
        try:
            exch = next(tokens)  # on exception it will be the default
            curr = next(tokens)  # on exception it will be the default

            if sectype == "FUT":
                if not expiry:
                    expiry = next(tokens)
                mult = next(tokens)

                # Try to see if this is FOP - Futures on OPTIONS
                right = next(tokens)
                # if still here this is a FOP and not a FUT
                sectype = "FOP"
                strike, mult = float(mult), ""  # assign to strike and void

                mult = next(tokens)  # try again to see if there is any

            elif sectype == "OPT":
                if not expiry:
                    expiry = next(tokens)
                strike = float(next(tokens))  # on exception - default
                right = next(tokens)  # on exception it will be the default

                mult = next(tokens)  # ?? no harm in any case

        except StopIteration:
            pass

        # Make the initial contract
        precon = self.ib.makecontract(
            symbol=symbol, sectype=sectype, exch=exch, curr=curr, expiry=expiry, strike=strike, right=right, mult=mult
        )

        return precon

    def start(self):
        """Starts the IB connecction and gets the real contract and
        contractdetails if it exists"""
        super(IBAData, self).start()
        self.ib.start(data=self)
        # The live subscription variables to be used with ib_async
        self.livefeed = Feed()
        self.histfeed = Feed(hist=True)

        if not self.ib.refreshing:
            self.do_refresh = True
            self.ib.refreshing = True

        self._usertvol = not self.p.rtbar
        tfcomp = (self._timeframe, self._compression)
        if tfcomp < self.RTBAR_MINSIZE:
            # Requested timeframe/compression not supported by rtbars
            self._usertvol = True

        self.contract = None
        self.contractdetails = None
        self.tradecontract = None
        self.tradecontractdetails = None

        if self.p.backfill_from is not None:
            self._state = self._ST_FROM
            self.p.backfill_from.setenvironment(self._env)
            self.p.backfill_from._start()
        else:
            self._state = self._ST_START  # initial state for _load
        self._statelivereconn = False  # if reconnecting in live state
        self._subcription_valid = False  # subscription state
        self._storedmsg = dict()  # keep pending live message (under None)

        if not self.ib.connected():
            return

        self.put_notification(self.CONNECTED)
        # get real contract details with real conId (contractId)
        cds = self.ib.getContractDetails(self.precontract, maxcount=1)
        if cds is not None:
            cdetails = cds[0]
            self.contract = cdetails.contract
            self.contractdetails = cdetails
        else:
            # no contract can be found (or many)
            self.put_notification(self.DISCONNECTED)
            return

        # create store dict after contract is setup
        self.ib.poststart(self)

        if self.pretradecontract is None:
            # no different trading asset - default to standard asset
            self.tradecontract = self.contract
            self.tradecontractdetails = self.contractdetails
        else:
            # different target asset (typical of some CDS products)
            # use other set of details
            cds = self.ib.getContractDetails(self.pretradecontract, maxcount=1)
            if cds is not None:
                cdetails = cds[0]
                self.tradecontract = cdetails.contract
                self.tradecontractdetails = cdetails
            else:
                # no contract can be found (or many)
                self.put_notification(self.DISCONNECTED)
                return

        if self._state == self._ST_START:
            self._start_finish()  # to finish initialization
            self._st_start()

    def stop(self):
        """Stops and tells the store to stop"""
        super(IBAData, self).stop()
        self.ib.stop()

    def feed_by_id(self, reqId):
        if self.livefeed.reqId == reqId:
            return self.livefeed
        elif self.histfeed.reqId == reqId:
            return self.histfeed
        else:
            raise KeyError("ReqId not found")

    def reqdata(self):
        """request real-time data. checks cash vs non-cash) and param useRT"""
        if self.contract is None or self._subcription_valid:
            return

        if self._usertvol:
            reqId, ticker = self.ib.reqMktData(self.contract)
        else:
            reqId, ticker = self.ib.reqRealTimeBars(self.contract, self.p.what, self.p.useRTH)
        self.livefeed.set_data(reqId, ticker)

        self._subcription_valid = True
        return self.livefeed

    def canceldata(self):
        """Cancels Market Data subscription, checking asset type and rtbar"""
        if self.contract is None:
            return

        if self._usertvol:
            self.ib.cancelMktData(self.contract)
        else:
            self.ib.cancelRealTimeBars(self.livefeed.data)

    def haslivedata(self):
        return bool(self._storedmsg or self.livefeed)

    def _load(self):
        if self.do_refresh and self._state != self._ST_HISTORBACK:  # refresh ib_async
            self.ib.conn.sleep(self.ib.p.refreshrate)

        if self.contract is None or self._state == self._ST_OVER:
            return False  # nothing can be done
        while True:
            if self._state == self._ST_LIVE:

                if self._storedmsg:
                    msg = self._storedmsg.pop(None, None)
                # First check for and handle errors
                elif self.livefeed.errors:
                    msg = self.livefeed.pop_error()
                # Once errors are handled we can load data
                elif self.livefeed.data:
                    # FIFO to mimic old queue system
                    msg = self.livefeed.pop_data()
                    if not msg:
                        return None
                else:
                    return None
                if msg is None:  # Conn broken during historical/backfilling
                    self._subcription_valid = False
                    self.put_notification(self.CONNBROKEN)
                    # Try to reconnect
                    if not self.ib.reconnect(resub=True):
                        self.put_notification(self.DISCONNECTED)
                        return False  # failed
                    self._statelivereconn = self.p.backfill
                    continue

                if msg in [-354, -420]:
                    self.put_notification(self.NOTSUBSCRIBED)
                    return False

                elif msg == -1100:  # conn broken
                    # Tell to wait for a message to do a backfill
                    # self._state = self._ST_DISCONN
                    self._subcription_valid = False
                    self._statelivereconn = self.p.backfill
                    continue

                elif msg == -1102:  # conn broken/restored tickerId maintained
                    # The message may be duplicated
                    if not self._statelivereconn:
                        self._statelivereconn = self.p.backfill
                    continue

                elif msg == -1101:  # conn broken/restored tickerId gone
                    # The message may be duplicated
                    self._subcription_valid = False
                    if not self._statelivereconn:
                        self._statelivereconn = self.p.backfill
                        self.reqdata()  # resubscribe
                    continue

                elif msg == -10225:  # Bust event occurred, current subscription is deactivated.
                    self._subcription_valid = False

                    if not self._statelivereconn:
                        self._statelivereconn = self.p.backfill
                        self.reqdata()  # resubscribe
                    continue

                elif isinstance(msg, integer_types):
                    # Unexpected notification for historical data skip it
                    # May be a "not connected not yet processed"
                    self.put_notification(self.UNKNOWN, msg)
                    continue

                # Process the message according to expected return type
                if not self._statelivereconn:
                    if self._laststatus != self.LIVE:
                        if len(self.livefeed) <= 1:  # very short live queue
                            self.put_notification(self.LIVE)

                    if self._usertvol:
                        ret = self._load_rtvolume(msg)
                    else:
                        ret = self._load_rtbar(msg)
                    if ret:
                        return True

                    # could not load bar ... go and get new one
                    continue

                # Fall through to processing reconnect - try to backfill
                self._storedmsg[None] = msg  # keep the msg

                # else do a backfill
                if self._laststatus != self.DELAYED:
                    self.put_notification(self.DELAYED)

                dtend = None
                if len(self) > 1:
                    # len == 1 ... forwarded for the 1st time
                    # get begin date in utc-like format like msg.datetime
                    dtbegin = num2date(self.datetime[-1])
                elif self.fromdate > float("-inf"):
                    dtbegin = num2date(self.fromdate)
                else:  # 1st bar and no begin set
                    # passing None to fetch max possible in 1 request
                    dtbegin = None
                # Convert msg.time to self._tz with proper offsets
                # msg.time comes in as UTC we will send as local by dropping tz
                dtend = msg.time.astimezone(self._tz).replace(tzinfo=None)
                reqId, ticker = self.ib.reqHistoricalDataEx(
                    contract=self.contract,
                    enddate=dtend,
                    begindate=dtbegin,
                    timeframe=self._timeframe,
                    compression=self._compression,
                    what=self.p.what,
                    useRTH=self.p.useRTH,
                )
                self.histfeed.set_data(reqId, ticker)

                self._state = self._ST_HISTORBACK
                self._statelivereconn = False  # no longer in live
                continue

            elif self._state == self._ST_HISTORBACK:

                if self.histfeed.errors:
                    msg = self.histfeed.pop_error()
                else:
                    msg = self.histfeed.pop_data()

                if msg is None:  # Conn broken during historical/backfilling
                    # Situation not managed. Simply bail out
                    self._subcription_valid = False
                    self.put_notification(self.DISCONNECTED)
                    return False  # error management cancelled the queue

                elif msg == -354:  # Data not subscribed
                    self._subcription_valid = False
                    self.put_notification(self.NOTSUBSCRIBED)
                    return False

                elif msg == -420:  # No permissions for the data
                    self._subcription_valid = False
                    self.put_notification(self.NOTSUBSCRIBED)
                    return False

                elif type(msg) is int:
                    # Unexpected notification for historical data skip it
                    # May be a "not connected not yet processed"
                    self.put_notification(self.UNKNOWN, msg)
                    continue

                if msg and msg.date is not None:
                    if self._load_rtbar(msg, hist=True):
                        return True  # loading worked
                    # the date is from overlapping historical request
                    continue
                # End of histdata
                if self.p.historical:  # only historical
                    self.put_notification(self.DISCONNECTED)
                    return False  # end of historical

                # Live is also wished - go for it
                self._state = self._ST_LIVE
                continue

            elif self._state == self._ST_FROM:
                if not self.p.backfill_from.next():
                    # additional data source is consumed
                    self._state = self._ST_START
                    continue

                # copy lines of the same name
                for alias in self.lines.getlinealiases():
                    lsrc = getattr(self.p.backfill_from.lines, alias)
                    ldst = getattr(self.lines, alias)

                    ldst[0] = lsrc[0]

                return True

            elif self._state == self._ST_START:
                if not self._st_start():
                    return False

    def _st_start(self):
        if self.p.historical:
            self.put_notification(self.DELAYED)
            dtend = None
            if self.todate < float("inf"):
                dtend = num2date(self.todate)

            dtbegin = None
            if self.fromdate > float("-inf"):
                dtbegin = num2date(self.fromdate)

            reqId, ticker = self.ib.reqHistoricalDataEx(
                contract=self.contract,
                enddate=dtend,
                begindate=dtbegin,
                timeframe=self._timeframe,
                compression=self._compression,
                what=self.p.what,
                useRTH=self.p.useRTH,
            )
            self.histfeed.set_data(reqId, ticker)

            self._state = self._ST_HISTORBACK
            return True  # continue before

        # Live is requested
        if not self.ib.reconnect(resub=True):
            self.put_notification(self.DISCONNECTED)
            self._state = self._ST_OVER
            return False  # failed - was so

        self._statelivereconn = self.p.backfill_start
        if self.p.backfill_start:
            self.put_notification(self.DELAYED)

        self._state = self._ST_LIVE
        return True  # no return before - implicit continue

    def _load_rtbar(self, rtbar, hist=False):
        # A complete 5 second bar made of real-time ticks is delivered and
        # contains open/high/low/close/volume prices
        # The historical data has the same data but with 'date' instead of
        # 'time' for datetime
        dt = date2num(rtbar.time if not hist else rtbar.date)
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # cannot deliver earlier than already delivered

        # Put the tick into the bar
        try:
            rtopen = rtbar.open
        except AttributeError:
            # rtbars use open_ for some reason
            rtopen = rtbar.open_

        self.lines.datetime[0] = dt
        self.lines.open[0] = rtopen
        self.lines.high[0] = rtbar.high
        self.lines.low[0] = rtbar.low
        self.lines.close[0] = rtbar.close
        self.lines.volume[0] = rtbar.volume
        self.lines.openinterest[0] = 0

        return True

    def _load_rtvolume(self, rtvol):
        # A single tick is delivered and is therefore used for the entire set
        # of prices. Ideally the
        # contains open/high/low/close/volume prices
        # Datetime transformation
        dt = date2num(rtvol.time)
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # cannot deliver earlier than already delivered

        self.lines.datetime[0] = dt

        # Put the tick into the bar
        tick = rtvol.ask
        self.lines.open[0] = tick
        self.lines.high[0] = tick
        self.lines.low[0] = tick
        self.lines.close[0] = tick
        self.lines.volume[0] = rtvol.askSize
        self.lines.openinterest[0] = 0

        return True
