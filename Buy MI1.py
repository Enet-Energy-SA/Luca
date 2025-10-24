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

prices = bids['Prices'].loc[:, ['period.1','NORD.1','CSUD.1','SUD.1','SICI.1']].rename(columns={'period.1': 'period','NORD.1': 'NORD','CSUD.1':'CSUD','SUD.1': 'SUD','SICI.1': 'SICI'})

pos_list = list(range(1,97)) + list(range(1,97)) + list(range(1,97))
purpose_list = ['Buy']*96*3
price_list = (prices.loc[:, 'NORD']-10).to_list() + (prices.loc[:, 'CSUD']-10).to_list() + (prices.loc[:, 'SUD']-10).to_list()
qty_list = [12]*96*3
area_code = ['NORD']*96 + ['CSUD']*96 + ['SUD']*96
unit_code = ['UC_DP2502_NORD']*96 + ['UC_DP2502_CSUD']*96 + ['UC_DP2502_SUD']*96
granularity = 'PT15M'
market = 'MI1'

trader.bid_auction(pos_list, purpose_list, price_list, qty_list, area_code, unit_code, granularity, market)

