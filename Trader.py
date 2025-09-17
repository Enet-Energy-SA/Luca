import requests
from datetime import date, datetime, timezone, timedelta
from typing import List
import sys
import json
import pandas as pd

class Trader:
    def __init__(self, username: str, password: str, base_url: str, flow_date: date):
        """
        Initialize the Trader and perform login.

        :param username: API username
        :param password: API password
        :param base_url: API base URL
        """
        self.username = username
        self.password = password
        self.base_url = base_url
        self.session = requests.Session()
        self.token = None

        flow_date = datetime.combine(flow_date, datetime.min.time(), tzinfo=timezone.utc)
        flow_date = flow_date - timedelta(hours=2)
        self.flow_date = flow_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Perform login immediately
        self.login()

    def login(self):
        """
        Perform login and store token/session.
        Adapt this function to your specific API requirements.
        """
        response = self.session.post(self.base_url + '/login', json={'username': self.username, 'password': self.password})
        response.raise_for_status()
        token = response.json()['token']
        self.session.headers['Authorization'] = f'Bearer {token}'
        print("Login successful.")
        return response.json()['user']['_id']

    def place_orders(self, zone: str, granularity: str, purpose_list: list, period: list, price: list, qty: list):
        """
        adds orders on the specific book
        """
        if (len(period) != len(price)) or (len(price)!= len(qty)):
            print('Dimensione prezzi e quantità discordanti')
            sys.exit()

        dim = len(period)

        if (granularity == 'PT15M') and dim > 96:
            print('Dimensione maggiore della granularità massima')
            sys.exit()
        elif (granularity != 'PT15M') and dim > 24:
            print('Dimensione maggiore della granularità massima')
            sys.exit()

        unit_code = 'UC_DP2502_' + zone

        payload = create_payload(pos_list=period, purpose_list=purpose_list, price_list=price, qty_list=qty,  area_code=zone, unit_code=unit_code, flow_date=self.flow_date, granularity=granularity)

        url = f"{self.base_url}/xbid/books/orders"
        response = self.session.post(url, json=payload)
        if response.status_code == 200:
            print("✅ Order posted successfully.")
            print(response.json())
        else:
            raise Exception(f"❌ Error {response.status_code}: {response.text}")

    def unrealized_pnl(self):

        query = {
            'resolution': 'PT15M',  # or 'PT15M' depending on your use case
        }

        response = self.session.request(
            'get',
            f"{self.base_url}/trades",
            params={
                'delivery_from': self.flow_date,
                'query$': json.dumps(query),
            }
        )

        trades = response.json().get('data', [])
        df = pd.DataFrame(trades)

        return df

def create_payload(pos_list: List[int], purpose_list: List[str], price_list: List[float], qty_list: List[float], area_code: str, unit_code: str, flow_date: str, granularity: str):
    """
    Creates the dictionary to be sent to the market via API
    """

    if not (len(pos_list) == len(purpose_list) == len(price_list) == len(qty_list)):
        raise ValueError("All input lists must have the same length")

    orders = []
    for pos, purpose, price, qty in zip(pos_list, purpose_list, price_list, qty_list):
        order = {
            "operationType": "NEW",
            "areaCode": area_code,
            "unitCode": unit_code,
            "flowDate": flow_date,
            "pos": pos,
            "resolution": granularity,
            "orderType": "STD",
            "mode": "NON",
            "executionType": "NOR",
            "purpose": purpose.upper(),
            "price": price,
            "qty": qty
        }
        orders.append(order)

    payload = {
        "orderType": "BSK",
        "orderExecutionType": "NON",
        "orders": orders
    }

    return payload

