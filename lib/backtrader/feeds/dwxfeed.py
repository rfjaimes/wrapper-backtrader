#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt
from backtrader.utils.py3 import (with_metaclass)
from backtrader.feed import DataBase
from backtrader.utils import date2num
import time
from datetime import datetime, timedelta
from pytz import timezone
from dateutil.tz import tzlocal
from lib.backtrader.stores.dwxstore import DWXStore

TIMEFRAMES = dict(
    (
        (bt.TimeFrame.Seconds, 's'),
        (bt.TimeFrame.Minutes, 'm'),
        (bt.TimeFrame.Days, 'd'),
        (bt.TimeFrame.Weeks, 'w'),
        (bt.TimeFrame.Months, 'm'),
        (bt.TimeFrame.Years, 'y'),
    )
)
    
class MetaDWXData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaDWXData, cls).__init__(name, bases, dct)

        # Register with the store
        DWXStore.DataCls = cls


class DWXData(with_metaclass(MetaDWXData, DataBase)):

    frompackages = (
        ('influxdb', [('InfluxDBClient', 'idbclient')]),
        ('influxdb.exceptions', 'InfluxDBClientError')
    )
    
    params = (
        ('influxdb', False),
        ('historical', False),
        ('saveticks', False),
        ('host', '127.0.0.1'),
        ('port', '8086'),
        ('username', None),
        ('password', None),
        ('database', None),
        ('timeframe', bt.TimeFrame.Days),
        ('startdate', None),
        ('high', 'high_p'),
        ('low', 'low_p'),
        ('open', 'open_p'),
        ('close', 'close_p'),
        ('volume', 'volume'),
        ('ointerest', 'oi'),
    )
    
    _store = DWXStore    
    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)
    
    def __init__(self, **kwargs):
        self.dwx = self._store(**kwargs)
        
    def setenvironment(self, env):
        '''Receives an environment (cerebro) and passes it over to the store it
        belongs to'''
        super(DWXData, self).setenvironment(env)
        env.addstore(self.dwx)        
    
    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        if self.p.historical:
            return False
        else:
            return True    

    def start(self):
        super(DWXData, self).start()
        
        self._state = self._ST_START
        self._broker_gmt = 3
        self._local_gmt = int(datetime.now(tzlocal()).strftime("%z")[0:3]) #int(tzlocal().tzname(datetime.now()))
        self._influxdb = self.p.influxdb
                
        
        if self.p.startdate:
            if self._influxdb:            
                self.getHistoInflux()
            else:
                self.getHistoDwx()     
            self._state = self._ST_HISTORBACK
        else:
            self._state = self._ST_LIVE
            
        if not self.p.historical:
            self.dwx._zmq._DWX_MTX_SUBSCRIBE_MARKETDATA_(self.p.dataname)        
    
    
        
    def getHistoInflux(self):
                    
        self.buf_ticks = []
        self.dt_lastSent = time.time()

        tf = '{multiple}{timeframe}'.format(
            multiple=(self.p.compression if self.p.compression else 1),
            timeframe=TIMEFRAMES.get(self.p.timeframe, 'd'))

        if self.p.startdate:
            dt_start = datetime.strptime(self.p.startdate, '%Y-%m-%d %H:%M:%S')
            utc = timezone('UTC')
            dt_utc = utc.localize(dt_start) 
            dt_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
            st = '>= \'%s\'' % dt_str

            if self.p.timeframe == bt.TimeFrame.Ticks:
                qstr = ('SELECT "{open_f}" AS "open", "{high_f}" AS "high", '
                    '"{low_f}" AS "low", "{close_f}" AS "close", '
                    '"{vol_f}" AS "volume", "{oi_f}" AS "openinterest" '
                    'FROM "{dataname}" '
                    'WHERE time {begin} ').format(
                        open_f=self.p.open, high_f=self.p.high,
                        low_f=self.p.low, close_f=self.p.close,
                        vol_f=self.p.volume, oi_f=self.p.ointerest,
                        begin=st, dataname=self.p.dataname)
            else:                
                # The query could already consider parameters like fromdate and todate
                # to have the database skip them and not the internal code
                qstr = ('SELECT mean("{open_f}") AS "open", mean("{high_f}") AS "high", '
                        'mean("{low_f}") AS "low", mean("{close_f}") AS "close", '
                        'mean("{vol_f}") AS "volume", mean("{oi_f}") AS "openinterest" '
                        'FROM "{dataname}" '
                        'WHERE time {begin} '
                        'GROUP BY time({timeframe}) fill(none)').format(
                            open_f=self.p.open, high_f=self.p.high,
                            low_f=self.p.low, close_f=self.p.close,
                            vol_f=self.p.volume, oi_f=self.p.ointerest,
                            timeframe=tf, begin=st, dataname=self.p.dataname)
    
            try:
                dbars = list(self.dwx._ndb.query(qstr).get_points())
            except InfluxDBClientError as err:
                print('InfluxDB query failed: %s' % err)
    
            self.biter = iter(dbars)  
            
            
            
    def getHistoDwx(self):
        broker_tz = self._broker_gmt
        local_tz = self._local_gmt
        dt_start_broker = datetime.strptime(self.p.startdate, '%Y-%m-%d %H:%M:%S') - timedelta(hours=local_tz-broker_tz)
        str_start_broker = dt_start_broker.strftime("%Y.%m.%d %H:%M:%S")
        dt_end_broker = datetime.now() - timedelta(hours=local_tz-broker_tz)
        str_end_broker = dt_end_broker.strftime("%Y.%m.%d %H:%M:%S")
        
        self.dwx._zmq._set_response_() #Clear de response
        self.dwx._zmq._DWX_MTX_SEND_MARKETDATA_REQUEST_(self.p.dataname, 1, str_start_broker, str_end_broker)
        
        print("getting history from Darwinex ...")
        count = 0
        limit = 6
        while not self.dwx._zmq._get_response_() and count <= limit:
            time.sleep(1)
            if not self.dwx._zmq._get_response_() and count%(limit/2)==0:
                self.dwx._zmq._DWX_MTX_SEND_MARKETDATA_REQUEST_(self.p.dataname, 1, str_start_broker, str_end_broker)
            count+=1
            
        respondZmq = self.dwx._zmq._get_response_()  
        respondZmq = respondZmq if respondZmq else []
        if '_data' in respondZmq:
            self._hist_data = respondZmq['_data']
            self.biter = iter(self._hist_data.items())        
        else:
            self.biter = iter([])
            print("cant get history from Darwinex")
        self._state = self._ST_HISTORBACK
        
    def setHistoInflux(self, bar):
        if not bar['time'].tzinfo:
            utc = timezone('UTC')
            dt_utc = utc.localize(bar['time'])
        else:
            dt_utc = bar['time'].astimezone(timezone('UTC'))        
        dt_str = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
              
        measurement = {}
        measurement['measurement'] = self.p.dataname
        measurement['tags'] = {}
        measurement['fields'] = {}
        measurement['tags']['symbol'] = self.p.dataname
        measurement['fields']['ask'] = bar['ask']
        measurement['fields']['bid'] = bar['bid']
        measurement['fields']['spread'] = bar['bid']
        measurement['fields']['open'] = bar['bid']
        measurement['fields']['close'] = bar['bid']
        measurement['fields']['high'] = bar['bid']
        measurement['fields']['low'] = bar['bid']
        measurement['fields']['real_volume'] = 0
        measurement["time"] = dt_str
        
        self.buf_ticks.append(measurement)        
        
        if time.time() - self.dt_lastSent > 60:       
            self.dt_lastSent = time.time()
            try:                
                if self.dwx._ndb.write_points(self.buf_ticks):
                    self.buf_ticks = []
            except InfluxDBClientError as err:
                print('setHistoInflux: InfluxDB write points failed: %s' % err)
        
        
        
    def _getNextTick_(self):
        bar=None
        if self.p.dataname in self.dwx._zmq._Market_Data_DB:
            if len(self.dwx._zmq._Market_Data_DB[self.p.dataname])>0:
                try:
                    firstDate = list(self.dwx._zmq._Market_Data_DB[self.p.dataname])[0]
                    if firstDate:                                                
                        dt_src = datetime.strptime(firstDate, '%Y-%m-%d %H:%M:%S.%f')
                        dt_src = timezone('UTC').localize(dt_src)
                        dt_local = dt_src.astimezone(timezone('America/Bogota'))
                        dt_str = dt_local.strftime("%Y-%m-%d %H:%M:%S %Z%z")
                        
                        firstData = self.dwx._zmq._Market_Data_DB[self.p.dataname].pop(firstDate)
                                               
                        bar = {'time':dt_local, 'bid': firstData[0], 'ask': firstData[1]}
                                                
                        #print('\r',dt_str,':',firstData)
                except Exception as e:
                    print('_getNetxt_ Error:', e)
            
        return bar
            
        
    def _load(self):
        if self._state == self._ST_LIVE:
            bar = self._getNextTick_()
            if bar is None:
                return None
            
            if self._influxdb and self.p.saveticks:
                self.setHistoInflux(bar)
            
            self.l.datetime[0] = date2num(bar['time'])
    
            self.l.open[0] = bar['bid']
            self.l.high[0] = bar['bid']
            self.l.low[0] = bar['bid']
            self.l.close[0] = bar['bid']
            self.l.volume[0] = 0.0
            self.l.openinterest[0] = 0.0
    
            return True                            
            
        elif self._state == self._ST_HISTORBACK:
            try:
                bar = next(self.biter)
            except StopIteration:
                print("Finish Historical Data")
                if self.p.historical:
                    print("End Feed data")
                    return False
                else:                    
                    print("Turn On Live Data")
                    self._state = self._ST_LIVE
                    return None
                
            if self._influxdb:
                local_tz = self._local_gmt
                
                dt_local = datetime.strptime(bar['time'].replace("Z","")[0:26],"%Y-%m-%dT%H:%M:%S.%f") # + timedelta(hours=local_tz)

                utc = timezone('UTC')
                dt_local = utc.localize(dt_local)
        
                self.l.datetime[0] = date2num(dt_local.astimezone())
        
                self.l.open[0] = bar['close']
                self.l.high[0] = bar['close']
                self.l.low[0] = bar['close']
                self.l.close[0] = bar['close']
                self.l.volume[0] = 0.0
                self.l.openinterest[0] = 0.0
                
            else:            
                broker_tz = self._broker_gmt
                local_tz = self._local_gmt
                dt_local = datetime.strptime(bar[0], '%Y.%m.%d %H:%M') + timedelta(hours=local_tz-broker_tz)
                #dt_str = dt_local.astimezone().strftime("%Y-%m-%d %H:%M:%SZ")
                self.l.datetime[0] = date2num(dt_local.astimezone())
        
                self.l.open[0] = bar[1]
                self.l.high[0] = bar[1]
                self.l.low[0] = bar[1]
                self.l.close[0] = bar[1]
                self.l.volume[0] = 0.0
                self.l.openinterest[0] = 0.0
    
            return True

        return False


    
    def stop(self):
        '''Stops and tells the store to stop'''
        super(DWXData, self).stop()
        if not self.p.historical:
            self.dwx._zmq._DWX_MTX_UNSUBSCRIBE_MARKETDATA_(self.p.dataname)

