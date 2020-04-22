from common.create_kite_session import *

kite = get_session()


def get_data(token, from_date, to_date, interval, historical_data, counter=0):
    try:
        return kite.historical_data(instrument_token=token, from_date=from_date, to_date=to_date, interval=interval)
    except:
        if counter <= 3:
            print("get_data exception ", counter)
            return get_data(token, from_date, to_date, interval, historical_data, counter + 1)
        else:
            print("returning old history_df ")
            return historical_data


def get_price(token, price, counter=0):
    try:
        return kite.quote(token)[token]['last_price']
    except:
        if counter <= 3:
            print("get_price exception ", counter)
            return get_price(token, price, counter + 1)
        else:
            print("returning old price ", price)
            return price


def get_price_v1(token, last_price_dict, counter=0):
    return_dict = {}
    try:
        price = kite.quote(token)[token]['last_price']
        for key in last_price_dict:
            return_dict[key] = price
        return return_dict
    except:
        if counter <= 3:
            print("get_price exception ", counter)
            return get_price(token, price, counter + 1)
        else:
            print("returning old price ", last_price_dict)
            return last_price_dict
