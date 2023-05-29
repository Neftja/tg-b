from tinkoff.invest import ( 
    StopOrderDirection,
    StopOrderExpirationType,
    StopOrderType,
    InstrumentIdType,
    OrderDirection,
    OrderType,
) 
from tinkoff.invest.exceptions import InvestError
from tinkoff.invest.utils import decimal_to_quotation, quotation_to_decimal
from decimal import Decimal
from loguru import logger



def stop_loss(client, figi: str, account_id):
    """
    Выставление STOP_LOSS по figi
    """


    min_price_increment = client.instruments.get_instrument_by(
        id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id=figi
    ).instrument.min_price_increment


    min_price_increment = quotation_to_decimal(min_price_increment)
    print(
    f"min_price_increment = {min_price_increment}"
    )
    
    price = quotation_to_decimal(price)

    calculated_price = price - price * Decimal(0.5 / 100)
    calculated_price = (
        round(calculated_price / min_price_increment) * min_price_increment
    )
    try:
        response = client.stop_orders.post_stop_order(
            figi=figi,
            quantity=1,
            price=decimal_to_quotation(Decimal(calculated_price)),
            stop_price=decimal_to_quotation(Decimal(calculated_price)),
            direction=StopOrderDirection.STOP_ORDER_DIRECTION_SELL,
            account_id=account_id,
            expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
            stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
            expire_date=None,
        )
        print(response)
        print("stop_order_id=", response.stop_order_id)
        return True
    except InvestError as error:
        logger.error("Ошибка постановки STOP_LOSS: %s", error)
    return False

def buy_share_market(client, figi: str, account_id):
    """
    Покупка figi по маркету
    """
    client.orders.post_order(figi=figi, 
                            quantity=1, 
                            direction=OrderDirection.ORDER_DIRECTION_BUY,
                            account_id=account_id,
                            order_type=OrderType.ORDER_TYPE_MARKET,
                            order_id='',
                            instrument_id='' )
    logger.info(f"Куплено {figi} по маркету 1 лот") 

def sell_share_limit(client, figi: str, account_id):
    """
    Продажа figi лимитированный сделкой
    """
    client.orders.post_order(figi=figi, 
                            quantity=1, 
                            direction=OrderDirection.ORDER_DIRECTION_SELL,
                            account_id=account_id,
                            order_type=OrderType.ORDER_TYPE_LIMIT,
                            order_id='',
                            instrument_id='' )
    logger.info(f"Продано {figi} по маркету 1 лот") 
