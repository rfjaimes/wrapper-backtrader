#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 14 13:32:00 2019

@author: rfjaimes
"""

import backtrader as bt

from strategy import Strategy


class Test_DWX(bt.Strategy):

    def __init__(self):
        super(Test_DWX, self).__init__()
        print('init Test_DWX')
        self.current_order = None
        self.last_order = None
        self.count_trade = 3
        self.count_step = 0

    def next(self):
        super(Test_DWX, self).next()

        self.count_step += 1
        
        if self.current_order:
            return 
        
        if self.count_step % 5 == 0:
            if not self.position:
                self.current_order = self.buy(data=self.data, size=0.01)
            else:
                self.current_order = self.close(data=self.data, order=self.last_order)

    def notify_trade(self,trade):
        super(Test_DWX, self).notify_trade(trade)

        if trade.isclosed:
            if self.last_order and '_open_price' in self.last_order.info and '_close_price' in self.last_order.info:
                _PnL_Real = self.last_order.info['_open_price']-self.last_order.info['_close_price']
                print("Pnl Real:", str(round(_PnL_Real,2)))
                
            self.count_trade -= 1
            if self.count_trade <= 0:
                print('---------------------------- END ---------------------------------')
                print('---------------------------- END ---------------------------------')
                print('---------------------------- END ---------------------------------')
                self.env.runstop()
                return

        self.last_order = self.current_order
        self.current_order = None

    def stop(self):
        from settings import CONFIG
        pnl = round(self.broker.getvalue() - CONFIG['capital_base'], 2)
        print('Test_DWX Period: Final PnL: {}'.format(pnl))


