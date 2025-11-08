import sys
sys.path.append(r"C:\Users\lbellomi\PycharmProjects\pythonProject\Trader")
from Trader import Trader
from datetime import date
import pandas as pd
import time

BASE_URL = "https://hermes.phinergy.biz/api"
USERNAME = "luca.bellomi"
PASSWORD = "FondaleAlgoso12!"

bids = pd.read_excel(r'C:\Users\lbellomi\PycharmProjects\pythonProject\Trader\bids.xlsm', sheet_name=None)

trader = Trader(USERNAME, PASSWORD, BASE_URL, flow_date=bids['Send Bids'].iloc[0,1].date())

granularity = []
zone = []
purpose =[]
period = []
price = []
qty = []
zone = []

keys = bids['Send Bids'].loc[:, 'Bidding Area']

if keys.values[0] == 'MI1':

    key = 'MI1'

    granularity = granularity + list(bids[key].loc[:, 'TIME'].values)
    purpose = purpose + list(bids[key].loc[:, 'PURPOSE'].values)
    period = period + list(bids[key].loc[:, 'PERIOD'].values)
    price = price + list(bids[key].loc[:, 'PRICE'].values)
    qty = qty + list(bids[key].loc[:, 'QTY'].values)
    zone = zone + list(bids[key].loc[:, 'ZONA'].values)

    trader.place_orders(zone=zone, granularity=granularity[0], purpose_list=purpose, period=[int(x) for x in period],
                        price=[float(x) for x in price], qty=[float(x) for x in qty], message=key)

elif keys.values[0][:4] == 'BIDS':

    n = keys.values[0].split('-')[1]
    cols = ['PURPOSE-' + n, 'PERIOD-' + n, 'PRICE-' + n, 'QTY-' + n, 'TIME-' + n, 'ZONA-' + n]

    bids = bids['BIDS'].loc[:, cols]

    bids = bids.dropna()

    granularity = granularity + list(bids.loc[:, cols[4]].values)
    purpose = purpose + list(bids.loc[:, cols[0]].values)
    period = period + list(bids.loc[:, cols[1]].values)
    price = price + list(bids.loc[:, cols[2]].values)
    qty = qty + list(bids.loc[:, cols[3]].values)
    zone = zone + list(bids.loc[:, cols[5]].values)

    trader.place_orders(zone=zone, granularity=granularity[0], purpose_list=purpose, period=[int(x) for x in period],
                        price=[float(x) for x in price], qty=[float(x) for x in qty], message=keys.values[0])

else:

    for key in keys:

        granularity = granularity + list(bids[key].loc[:, 'TIME'].values)
        purpose = purpose + list(bids[key].loc[:, 'PURPOSE'].values)
        period = period + list(bids[key].loc[:, 'PERIOD'].values)
        price = price + list(bids[key].loc[:, 'PRICE'].values)
        qty = qty + list(bids[key].loc[:, 'QTY'].values)
        zone = zone + [key[:-2]]*len(list(bids[key].loc[:, 'QTY'].values))

    trader.place_orders(zone=zone, granularity=granularity[0], purpose_list=purpose, period=[int(x) for x in period],
                        price=[float(x) for x in price], qty=[float(x) for x in qty], message=key)

