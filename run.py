import backtrader as bt

from utils import run_strategy, add_analyzers
from settings import CONFIG

import datetime
from datetime import timedelta
from dateutil.tz import tzlocal
from lib.backtrader.stores.dwxstore import DWXStore
from lib.backtrader.feeds.dwxfeed import DWXData
from lib.backtrader.brokers.dwxbroker import DWXBroker


cerebro = bt.Cerebro(runonce=False, live=True)

# Data input
#data = bt.feeds.YahooFinanceData(dataname=CONFIG['asset'],
#                                 fromdate=CONFIG['init_date'],
#                                 todate=CONFIG['end_date'])

# MT4 DWX data
dt_start = datetime.datetime.now() - timedelta(hours=1)  
str_start = dt_start.strftime("%Y-%m-%d %H:%M:%S")   #Traemos datos de la ultima hora
        
storekwargs = dict(
        database='dwx',                 #influx Database
        influxdb=False,                 #Deshabilitar Influxdb
        host='localhost',               #MT4 host
        hostdb = 'localhost',           #host influxdb - podria ser diferente del MT
        historical= False               #True si no queremos trading en vivo
)

datakwargs = dict(
        dataname='EURUSD',              #Por ahora solo soporta de a un simbolo
        saveticks= False,               #True si queremos guardar la data recibida de MT en influxDB
        timeframe= bt.TimeFrame.Ticks,
        #startdate= str_start,           #'2019-05-10 12:00:00' con el GMT local
        compression=1,

        high='high',                        #influx field del measurement en InfluxDB 
        low='low',                          #influx field del measurement en InfluxDB 
        open='open',                        #influx field del measurement en InfluxDB 
        close='close',                      #influx field del measurement en InfluxDB 
        volume='real_volume',               #influx field del measurement en InfluxDB 
        ointerest='tick_volume'             #influx field del measurement en InfluxDB 
)

datakwargs.update(storekwargs)

dwxstore = DWXStore(**storekwargs)
broker = dwxstore.getbroker()
data = dwxstore.getdata(**datakwargs)

cerebro.setbroker(broker)
cerebro.adddata(data)

if CONFIG['mode'] == 'optimization':
    # Parameters Optimization
    for strat in CONFIG['strategies']:
        cerebro.optstrategy(strat, period=range(14,21))
elif CONFIG['mode'] == 'backtest':
    for strat in CONFIG['strategies']:
        cerebro.addstrategy(strat)
else:
    raise ValueError('CONFIG["mode"] value should be "backtest", "optimization" or "walk_forward".')

# Analyzer
#cerebro = add_analyzers(cerebro)

# Set our desired cash start
cerebro.broker.setcash(CONFIG['capital_base'])

# Add a FixedSize sizer according to the stake
cerebro.addsizer(bt.sizers.FixedSize, stake=5)

# Set the commission
cerebro.broker.setcommission(commission=CONFIG['commission'])

# Run Strategy
strats = run_strategy(cerebro)

if CONFIG['plot'] and CONFIG['mode'] != 'optimization':
    # Plot the result
    cerebro.plot()
