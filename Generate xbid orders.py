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

trader.fetch_auction_new()
trader.generate_bids_new()

