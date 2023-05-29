#from celery import Celery 
import asyncio 
import conf 
from datetime import timedelta, datetime
import pandas as pd
from tinkoff.invest import ( 
    AsyncClient, 
    MarketDataRequest, 
    SubscriptionAction, 
    SubscribeTradesRequest, 
    TradeInstrument,
    SubscribeOrderBookRequest,
    OrderBookInstrument,
) 
#from sqlalchemy.orm import Session 
#from sqlalchemy import create_engine 
#import postgresql_tables 
import telegram_bot 
from loguru import logger
import post_order
import time
from tinkoff.invest.utils import quotation_to_decimal
logger.add("error.log", format="{time} {level} {message}", level="ERROR")
logger.add("info.log", format="{time} {level} {message}", level="INFO")
#app = Celery('tasks', broker='redis://172.29.112.1', result_backend='db+sqlite:////home/user/Trading_bot/file.db') 

################### 



#engine = create_engine(f"postgresql+psycopg2://postgres:{conf.POSTGRESQL_PASS}@172.29.112.1:5432/postgres") 
#connection = engine.raw_connection() 
#cursor = connection.cursor() 

TOKEN = conf.TOKEN_T
ACCOUNT_ID = conf.account_id

async def send_message(text: str) -> None: 
    await telegram_bot.main(text=text) 


def time_job():
    pass
########################################################### Тут блок записи в БД данных ####################################
 # надо добавить лог входа с вделки

def write_df_orders(df: pd.DataFrame):
    # передовать по 200 и более ордеров для записи в бд
    # наверное это самый быстрый вариант
    pass

# async def write_share_orders(stream_orders: dict) -> None: 
#     """ 
#     dict_candles['Direction'] принимает значение 1 или 2, что соответствует buy и sell 
#     """      
#     cursor.execute(f"""INSERT INTO public."Orders_{stream_orders['FIGI']}" (direction, price, quantity, tim) 
#                        VALUES ('{stream_orders['Direction']}', '{stream_orders['Price']}', '{stream_orders['Quantity']}', '{stream_orders['Time']}');""") 
#     connection.commit() 


# async def write_share_candles(stream_sdelki: dict, figi: str) -> None: 
#     cursor.execute(f"""INSERT INTO public."Candle_{figi}" (open, high, low, close, volume, tim) 
#                        VALUES ('{stream_sdelki['Open']}', '{stream_sdelki['High']}', '{stream_sdelki['Low']}', '{stream_sdelki['Close']}', '{stream_sdelki['Volume']}', '{stream_sdelki['last_trade_ts']}');""") 
#     connection.commit() 


################################################## ТУТ ДОЛЖЕН БЫТЬ ОСНОВНОЙ БЛОК ####################################### 
def see_dominance_orders(direction: str, quantity: int, mas_direction_price: dict[dict[datetime, dict[str, int]]], times: datetime):
    """
    Доминация продовца или покупатиля  
        mas_direction_price = {'time_orders': {
                                '': 
                                    {'buy': 1,
                                    'sell': 1}}}
    """

    if direction == direction.TRADE_DIRECTION_BUY:
        mas_direction_price['time_orders'][times]['buy'] += quantity
        #mas_direction_price['1'] += quantity
    else:
        mas_direction_price['time_orders'][times]['sell'] += quantity
        #mas_direction_price['2'] += quantity
    ################################# блок условий для вывода доминации за текущую минуту ################
    if mas_direction_price['time_orders'][times]['buy'] > mas_direction_price['time_orders'][times]['sell']:
        print("Buy > Sell",mas_direction_price['time_orders'][times]['buy'] / mas_direction_price['time_orders'][times]['sell'])

    elif mas_direction_price['time_orders'][times]['sell'] > mas_direction_price['time_orders'][times]['buy']:
        print("Sell > Buy", mas_direction_price['time_orders'][times]['sell'] / mas_direction_price['time_orders'][times]['buy'])
    ######################################################################################################
    print(mas_direction_price)


def zero_point_search(price: float, times: int) -> int | float: 
    """
    Тут просто точка отчета
    """ 
    time_zero_order = times 
    price_zero_order = price 

    return time_zero_order, price_zero_order 

def find_up_or_down_candle(flag_find_up_candle: bool, flag_find_down_candle: bool, price: float, price_zero_order: float) -> bool | float:
    koef_up = 0.001 # = 0,1%
    # неправильно считает ?
    pricce_up = price_zero_order + price_zero_order * koef_up
    pricce_down = price_zero_order - price_zero_order * koef_up
    #pricce_up_enter_position = 0
    #pricce_down_enter_position = 0

    if pricce_up < price:
        pricce_up_enter_position = price 
        logger.info(f"Покупка в LONG по цене: {price}")
        # тут должны закупиться и поставить STOP_LOSS 
        # сколько на это понадобится времени??
        #post_order.buy_share_market(client, figi, account_id)
        #post_order.stop_loss(client, figi, account_id)
        flag_find_up_candle = True
    
    elif pricce_down > price:
        pricce_down_enter_position = price
        flag_find_down_candle = True
        logger.info(f"Покупка в SHORT по цене: {price}")

    return flag_find_up_candle, flag_find_down_candle, pricce_up_enter_position, pricce_down_enter_position



def chek_time_candle(time_zero_order: int, price_zero_order: float, times: int, price: float) -> int | float: 
    """
    Разбиваем поток на минуты. 
    Устанавливаем цену на первую сделку для этой минуты для точки отчета изменения цены
    """
    
    if time_zero_order < times or (time_zero_order == 59 and times == 00): 
        time_zero_order = times 
        price_zero_order = price 
        logger.info(f"price_zero_order -> {price_zero_order} ")
    return time_zero_order, price_zero_order

def find_close_long_position(price: float, pricce_up_enter_position: float, max_prise: float, flag_find_up_candle: bool) -> bool:
    """
    есть 2 стротегии:
    - резкое повышение цены
    - следить за последними 2 - 3 свечками / стоит ли?

    """
        
    if max_prise < price:
        max_prise = price
        logger.info(f"Обновление максимума, цена {price}, держим позицию.")
    
    elif max_prise > price:
        # прежде чем закрыть, необходимо снять STOP_LOSS
        # тут необходимо выставить коэф - ниже которого происходит продажа
        # продажа по лимит ордеру
        price_down: float = (max_prise - price) / max_prise * 100
        logger.info(f'Падение цены на {round(price_down, 3)} %')
        #post_order.sell_share_limit(client, figi, account_id)
        if pricce_up_enter_position > price:
            flag_find_up_candle = False
            logger.info(f'Закрыли позицию по цене {price}')
    return flag_find_up_candle, max_prise

def find_close_short_position(price: float, pricce_down_enter_position: float, min_prise: float, flag_find_down_candle: bool):
    
    if min_prise > price:
        min_prise = price
        logger.info(f"Обновление минимума, цена {price}, держим позицию.")
    
    elif min_prise < price:
        # прежде чем закрыть, необходимо снять STOP_LOSS
        # тут необходимо выставить коэф - ниже которого происходит продажа
        # продажа по лимит ордеру
        price_up: float = (price - min_prise) / min_prise * 100
        logger.info(f'Повышение цены на {round(price_up, 3)} %')
        #post_order.sell_share_limit(client, figi, account_id)
        if pricce_down_enter_position < price:
            flag_find_down_candle = False
            logger.info(f'Закрыли позицию по цене {price}')
    return flag_find_down_candle, min_prise
####################################################################################################################### 

async def request_iterator(kwargs): 
    yield MarketDataRequest( 
            subscribe_trades_request=SubscribeTradesRequest( 
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE, 
            instruments=[ 
                TradeInstrument( 
                    figi=kwargs['figis']                         
                ) 
            ], 
        ) 
    ) 
    # yield MarketDataRequest(
    #         subscribe_order_book_request=SubscribeOrderBookRequest(
    #         subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
    #         instruments=[
    #             OrderBookInstrument(
    #                 figi=kwargs['figis'],
    #                 depth=50
    #             )
    #         ]
    #     )
    # )

    while True: 
        await asyncio.sleep(.1) 


async def main(kwargs): 
    flag_start_programm = False 
    flag_find_up_candle = False
    flag_find_down_candle = False
    flag_send_message = False 
    max_prise = 0
    min_prise = 0
    pricce_up_enter_position = 0
    pricce_down_enter_position = 0
    mas_direction_price = {'time_orders': {
                                        '': 
                                        {'buy': 1,
                                        'sell': 1}}}

    async with AsyncClient(TOKEN) as client:    
        async for marketdata in client.market_data_stream.market_data_stream( 
            request_iterator(kwargs) 
        ):   
            # пересмотреть логику работы с флагом 

            if marketdata.trade != None: 
                figi = marketdata.trade.figi
                datetimes = (marketdata.trade.time + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
                times = int(datetimes[-5:-3])
                price = float(quotation_to_decimal(marketdata.trade.price))
                instrument_uid = marketdata.trade.instrument_uid
                direction = marketdata.trade.direction
                quantity = marketdata.trade.quantity 
                print(datetimes, price, quantity, instrument_uid)

                if not flag_start_programm: 
                    time_zero_order, price_zero_order = zero_point_search(price, times) 
                    flag_start_programm = True 

                if not flag_find_up_candle:
                    time_zero_order, price_zero_order = chek_time_candle(time_zero_order, price_zero_order, times, price)     
                    flag_find_up_candle, flag_find_down_candle, pricce_up_enter_position, pricce_down_enter_position = find_up_or_down_candle(flag_find_up_candle, flag_find_down_candle, price, price_zero_order)

                elif flag_find_up_candle:
                    if not flag_send_message:
                        await telegram_bot.main(text=f"Открыта позиция в LONG по {figi}")
                        flag_send_message = True

                    flag_find_up_candle, max_prise = find_close_long_position(price, pricce_up_enter_position, max_prise, flag_find_up_candle)
                
                elif flag_find_down_candle:
                    if not flag_send_message:
                        await telegram_bot.main(text=f"Открыта позиция в SHORT по {figi}")
                        flag_send_message = True

                    flag_find_down_candle, min_prise = find_close_short_position(price, pricce_down_enter_position, min_prise, flag_find_down_candle)
                    #see_dominance_orders(direction, quantity, mas_direction_price)


# @app.task(track_started = True) 
# def celery_task(**kwargs): 
#     loop = asyncio.get_event_loop() 
#     return loop.run_until_complete(write_shares(**kwargs)) 

# запись в файл csv построчно 
# запись в файл csv датафрейм 
# запись в бд
kwargs = {}
kwargs['figis'] = 'BBG000RG4ZQ4'

if __name__ == "__main__":
    asyncio.run(main(kwargs))
