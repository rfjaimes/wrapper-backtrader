#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 14 13:32:00 2019

@author: rfjaimes
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt
from backtrader.utils.py3 import (with_metaclass)
import time
from lib.backtrader.stores.dwxstore import DWXStore


class MetaDWXBroker(bt.brokers.BackBroker.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaDWXBroker, cls).__init__(name, bases, dct)
        DWXStore.BrokerCls = cls


class DWXBroker(with_metaclass(MetaDWXBroker, bt.brokers.BackBroker)):
    
    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)
    
    def __init__(self, **kwargs):
        super(DWXBroker, self).__init__()
        self.dwx = DWXStore(**kwargs)
    
    def init(self):
        super(DWXBroker, self).init()
        #self._ST_LIVE = self._ST_HISTORBACK        
        
    def getDWX(self):
        pass
    
    def buy(self, owner, data,
            size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0,
            **kwargs):
        
        result = super(DWXBroker, self).buy(owner, data,
            size, price, plimit,
            exectype, valid, tradeid,
            **kwargs)
        
        if 'order' in kwargs:
            #Cuando se recibe una orden proviene de un metodo close para cerrar la orden
            self.close(data, kwargs['order'])
            return result
        
        if data._state == data._ST_LIVE:
            print("DWXBroker::buy ", result)
            
            _my_trade = self.dwx._zmq._generate_default_order_dict()
            _my_trade['_type'] = 0 # 0-Buy
            _my_trade['_lots'] = size
            _my_trade['_SL'] = 0
            _my_trade['_TP'] = 0
            _my_trade['_symbol'] = data._dataname
            
            self.dwx._zmq._set_response_() #Clear de response
            self.dwx._zmq._DWX_MTX_NEW_TRADE_(_order=_my_trade)
            
            while not self.dwx._zmq._get_response_():
                time.sleep(1)
                
            respondZmq = self.dwx._zmq._get_response_()     
            print("DWXBroker::buy.result: ", respondZmq)
            
            if '_ticket' in respondZmq:
                result.info['_ticket'] = respondZmq['_ticket']
            else:
                print("Can't create order")

        return result

    def sell(self, owner, data,
             size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0,
             **kwargs):

        result = super(DWXBroker, self).sell(owner, data,
             size, price, plimit,
             exectype, valid, tradeid,
             **kwargs)
        
        if 'order' in kwargs:
            #Cuando se recibe una orden proviene de un metodo close para cerrar la orden
            self.close(data, kwargs['order'])
            return result
        

        if data._state == data._ST_LIVE:           
            print("DWXBroker::sell ", result)
            
            _my_trade = self.dwx._zmq._generate_default_order_dict()
            _my_trade['_type'] = 1 # 1-Sell
            _my_trade['_lots'] = size
            _my_trade['_SL'] = 0
            _my_trade['_TP'] = 0
            _my_trade['_symbol'] = data._dataname
            
            self.dwx._zmq._set_response_() #Clear de response
            self.dwx._zmq._DWX_MTX_NEW_TRADE_(_order=_my_trade)
            
            while not self.dwx._zmq._get_response_():
                time.sleep(1)
                
            respondZmq = self.dwx._zmq._get_response_()     
            print("DWXBroker::sell.result: ", respondZmq)       
            
            if '_ticket' in respondZmq:
                result.info['_ticket'] = respondZmq['_ticket']
                result.info['_open_price'] = respondZmq['_open_price']
            else:
                print("Can't create order")
            

        return result

    def cancel(self, order, bracket=False):
        result = super(DWXBroker, self).cancel(order, bracket)
        
        print("DWXBroker::cancel", result)
            
        return result
    
    
    def close(self, data, order):
        if data._state == data._ST_LIVE:
            print("DWXBroker::close ")
            
            if '_ticket' in order.info:
                
                self.dwx._zmq._set_response_() #Clear de response
                self.dwx._zmq._DWX_MTX_CLOSE_TRADE_BY_TICKET_(_ticket=order.info['_ticket'])
                
                while not self.dwx._zmq._get_response_():
                    time.sleep(1)
                    
                respondZmq = self.dwx._zmq._get_response_()  
                if '_ticket' in respondZmq:
                    order.info['_close_price'] = respondZmq['_close_price']
                 
                print("DWXBroker::close.result: ", respondZmq)
                
            else:
                print("Can't close order")
            
        return None
        

        
        
        
    
    