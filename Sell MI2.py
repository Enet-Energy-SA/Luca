import sys
sys.path.append(r"C:\Users\lbellomi\PycharmProjects\pythonProject\Trader")
from Trader import Trader
from datetime import date
import pandas as pd
import time

BASE_URL = "https://hermes.phinergy.biz/api"
USERNAME = "luca.bellomi"
PASSWORD = "OceanosenzaMare44!"

bids = pd.read_excel(r'C:\Users\lbellomi\PycharmProjects\pythonProject\Trader\bids.xlsm', sheet_name=None)
trader = Trader(USERNAME, PASSWORD, BASE_URL, flow_date=bids['Send Bids'].iloc[0,1].date())

portfolios = ['NORD', 'CSUD']

df = trader.imbalance_position(unit_id=portfolios)

pos_list = []
purpose_list = []
price_list = []
qty_list = []
area_code = []
unit_code = []
granularity = 'PT15M'
market = 'MI2'

for zona in df.loc[:, 'zone'].unique():

    n = df.loc[df['zone'] == zona, 'period'].shape[0]
    pos_list = pos_list + df.loc[df['zone'] == zona, 'period'].astype(int).tolist()
    purpose_list = purpose_list + ['Sell']*n
    price_list = price_list + [0]*n
    qty_list = qty_list + df.loc[df['zone'] == zona, 'commercial_imbalance'].apply(lambda n: n*4).astype(float).tolist()
    area_code = area_code + [zona]*n
    unit_code = unit_code + ['UC_DP2502_' + zona]*n

trader.bid_auction(pos_list, purpose_list, price_list, qty_list, area_code, unit_code, granularity, market)

