import sys
sys.path.append(r"C:\Users\lbellomi\PycharmProjects\pythonProject\Trader")

from Trader import Trader
from datetime import date
import pandas as pd

import time

BASE_URL = "https://hermes.phinergy.biz/api"
USERNAME = "usr"
PASSWORD = "pass"

trader = Trader(USERNAME, PASSWORD, BASE_URL)

flow_date = date(2025, 9, 16)

bids = pd.read_excel(r'C:\Users\lbellomi\PycharmProjects\pythonProject\Trader\bids.xlsx', sheet_name=None)

granularity = {}
zone = {}
purpose = {}
period = {}
price = {}
qty = {}

for key in bids.keys():

    zone[key] = key[:-2]
    granularity[key] = list(bids[key].loc[:, 'TIME'].values)
    purpose[key] = list(bids[key].loc[:, 'PURPOSE'].values)
    period[key] = list(bids[key].loc[:, 'PERIOD'].values)
    price[key] = list(bids[key].loc[:, 'PRICE'].values)
    qty[key] = list(bids[key].loc[:, 'QTY'].values)

    trader.place_orders(zone=zone[key], granularity=granularity[key][0], purpose_list=purpose[key], flow_date=flow_date, period=[int(x) for x in period[key]],
                        price=[float(x) for x in price[key]], qty=[float(x) for x in qty[key]])

    time.sleep(5)
