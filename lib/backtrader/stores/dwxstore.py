#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 14 13:32:00 2019

@author: rfjaimes
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt
import datetime as dt
import time
import collections
from datetime import datetime, timedelta
from pytz import timezone
from dateutil.tz import tzlocal
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import (with_metaclass)

import sys
sys.path.append('vendor/dwx-zeromq-connector/v2.0.1/python/api')
from DWX_ZeroMQ_Connector_v2_0_1_RC8 import DWX_ZeroMQ_Connector


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class DWXStore(with_metaclass(MetaSingleton, object)):
    frompackages = (
        ('influxdb', [('InfluxDBClient', 'idbclient')]),
        ('influxdb.exceptions', 'InfluxDBClientError')
    )
    
    params = (
        ('influxdb', False),
        ('historical', False),
        ('host', '127.0.0.1'),
        ('hostdb', None),
        ('port', '8086'),
        ('username', None),
        ('password', None),
        ('database', None)
    )

    
    def __init__(self):
        super(DWXStore, self).__init__()
        self._connected = False  # modules/objects created
        
        if not self.p.historical or (self.p.startdate and not self._influxdb):
            self.initDwx()
            self._connected = True 
        
        if self.p.influxdb: 
            self.initInflux()
            self._connected = True 
            
        
        
    def initDwx(self):
        #self._symbols = [(self.p.dataname,0.01)]
        self._verbose=True
        
    
        self._name = "DWXData"
        self._market_open = True
        self._delay = 0.5        
        self._zmq = DWX_ZeroMQ_Connector(_host=self.p.host, _verbose=self._verbose)
        
        self._zmq._set_response_() #Clear de response
        self._zmq._DWX_MTX_GET_ALL_OPEN_TRADES_()
        
        print("getting all open trades...")
        while not self._zmq._get_response_():
            time.sleep(1)
            
        respondZmq = self._zmq._get_response_()          
        
        
        self._zmq._DWX_MTX_CLOSE_TRADES_BY_MAGIC_(123456)

        print("close all open trades...")
        while not self._zmq._get_response_():
            time.sleep(1)
            
        respondZmq = self._zmq._get_response_()          

        

    def initInflux(self):
        _hostdb = self.p.hostdb if self.p.hostdb else self.p.host
        
        try:
            self._ndb = idbclient(_hostdb, self.p.port, self.p.username,
                                 self.p.password, self.p.database)
        except InfluxDBClientError as err:
            print('Failed to establish connection to InfluxDB: %s' % err)          
        
        
        
        
        
    '''Base class for all Stores'''

    _started = False


    def getdata(self, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        data = self.DataCls(*args, **kwargs)
        data._store = self
        return data

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        broker = cls.BrokerCls(*args, **kwargs)
        broker._store = cls
        return broker

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    def start(self, data=None, broker=None):
        if not self._started:
            self._started = True
            self.notifs = collections.deque()
            self.datas = list()
            self.broker = None

        if data is not None:
            self._cerebro = self._env = data._env
            self.datas.append(data)

            if self.broker is not None:
                if hasattr(self.broker, 'data_started'):
                    self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker

    def stop(self):
        pass         


    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        '''Return the pending "store" notifications'''
        self.notifs.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifs.popleft, None)]       
        
        

        
        
    
    