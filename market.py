import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров из нашего магазина в Yandex.Market.

    Attributes:
        page (str): Идентификатор страницы выдачи API, которую нужно открыть.
        campaign_id (str): Идентификатор магазина для API Yandex.Market.
        access_token (str): Токен доступа к API для Yandex.Market.

    Returns:
        dict: Часть ответа API, содержащая список товаров.
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить данные товаров нашего интернет-магазина.

    Attributes:
        stocks (list): Список товаров с обновлёнными данными.
        campaign_id (str): Идентификатор магазина для API Yandex.Market.
        access_token (str): Токен доступа к API для Yandex.Market.

    Returns:
        dict: Ответ API

    Examples:
        >>> env = Env()
        >>> stocks = [{
        ...     "sku": 48852,
        ...     "warehouseId": 1234567,
        ...     "items": [
        ...         {
        ...             "count": 3,
        ...             "type": "FIT",
        ...             "updatedAt": str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"),
        ...         }
        ...     ],
        ... },
        ... ...]
        >>> update_stocks(stocks, env.str("FBS_ID"), env.str("MARKET_TOKEN"))
        Returns response from Yandex.Market.

    Raises:
        requests.exceptions.HTTPError
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров нашего интернет-магазина.

    Attributes:
        prices (list): Список товаров с обновлёнными данными.
        campaign_id (str): Идентификатор магазина для API Yandex.Market.
        access_token (str): Токен доступа к API для Yandex.Market.

    Returns:
        dict: Ответ API

    Examples:
        >>> env = Env()
        >>> some_prices = [{
        ...                     id: 12345,
        ...                     price{
        ...                         value: 5990,
        ...                         currencyId: "RUR",
        ...                     }
        ...                 },
        ...                 ...
        ...                 ]
        >>> update_price(some_prices, env.str("FBS_ID"), env.str("MARKET_TOKEN"))
        Return the response described here:
        https://yandex.com/dev/market/partner/doc/dg/reference/post-campaigns-id-offer-prices-updates.html

    Raises:
        requests.exceptions.HTTPError
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета

    Attributes:
        campaign_id (str): Идентификатор магазина для API Yandex.Market.
        market_token (str): Токен доступа к API для Yandex.Market.

    Returns:
        list: Список артикулов товаров нашего магазина.

    Examples:
        >>> env = Env()
        >>> get_offer_ids(env.str("FBS_ID"), env.str("MARKET_TOKEN"))
        [123, 345, ...]

    Raises:
        requests.exceptions.HTTPError
    """

    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Подготовить данные товаров для загрузки в магазин Yandex.Market

    Arguments:
        watch_remnants (list): Список словарей с данными о товарах.
        offer_ids (list): Список имеющихся в магазине товаров.
        warehouse_id (str): Идентификатор склада для API Yandex.Market

    Returns:
        list: Список обновлённых данных о товарах для магазина Yandex.Market.

    Examples:
        >>> offer_ids = [123, 345, ...]
        >>> watch_remnants = [{
        ...                     "Код": 48852,
        ...                     "Наименование товара": "B 4204 LSSF",
        ...                     "Изображение": "http://...",
        ...                     "Цена": "24'570.00 руб.",
        ...                     "Количество": 3,
        ...                     "Заказ": ,
        ...                 },
        ...                 ...
        ...                 ]
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        [{
            "sku": 48852),
            "warehouseId": 1234567,
            "items": [
                {
                    "count": 3,
                    "type": "FIT",
                    "updatedAt": str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"),
                }
            ],
        },
        ...]

    Raises:
        requests.exceptions.HTTPError
    """

    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Обновить цены для товаров.

    Arguments:
        watch_remnants (list): Список словарей с данными о товарах.
        offer_ids (list): Список имеющихся в магазине товаров.

    Returns:
        list: Список товаров с обновлёнными ценами.

    Examples:
        >>> offer_ids = [123, 345, ...]
        >>> watch_remnants = [{
        ...                     "Код": 48852,
        ...                     "Наименование товара": "B 4204 LSSF",
        ...                     "Изображение": "http://...",
        ...                     "Цена": "24'570.00 руб.",
        ...                     "Количество": 3,
        ...                     "Заказ": ,
        ...                 },
        ...                 ...
        ...                 ]
        >>> create_prices(watch_remnants, offer_ids)
        [{
            id: 12345,
            price{
                value: 5990,
                currencyId: "RUR",
            }
        },
        ...
        ]
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить новые цены товаров в магазин Yandex.Market.

    Arguments:
        watch_remnants (list): Список словарей с данными о товарах.
        campaign_id (str): Идентификатор клиента, сгенерированный для доступа к API Yandex.Market.
        market_token (str): Токен, сгенерированный для доступа к API Yandex.Market.

    Returns:
        list: Цены, которые были загружены.

    Examples:
        >>> watch_remnants = [{
        ...                     "Код": 48852,
        ...                     "Наименование товара": "B 4204 LSSF",
        ...                     "Изображение": "http://...",
        ...                     "Цена": "24'570.00 руб.",
        ...                     "Количество": 3,
        ...                     "Заказ": ,
        ...                 },
        ...                 ...
        ...                 ]
        >>> upload_prices(watch_remnants, env.str("FBS_ID"), env.str("MARKET_TOKEN"))
        [{
            id: 12345,
            price{
                value: 5990,
                currencyId: "RUR",
            }
        },
        ...
        ]
    """

    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Обновить данные товарах в магазине Yandex.Market.

    Arguments:
        watch_remnants (list): Список словарей с данными о товарах.
        campaign_id (str): Идентификатор клиента, сгенерированный для доступа к API Yandex.Market.
        market_token (str): Токен, сгенерированный для доступа к API Yandex.Market.
        warehouse_id (str): Идентификатор склада для API Yandex.Market.

    Returns:
        list: Список товаров, которые ещё остались в продаже.
        list: Список данных о товарах, которые были обновлены.

    Examples:
        >>> env = Env()
        >>> watch_remnants = [{
        ...                     "Код": 48852,
        ...                     "Наименование товара": "B 4204 LSSF",
        ...                     "Изображение": "http://...",
        ...                     "Цена": "24'570.00 руб.",
        ...                     "Количество": 3,
        ...                     "Заказ": ,
        ...                 },
        ...                 ...
        ...                 ]
        >>> upload_stocks(watch_remnants, env.str("FBS_ID"), env.str("MARKET_TOKEN"))
        Два списка с подобной структурой:
        [{
            "sku": 48852),
            "warehouseId": 1234567,
            "items": [
                {
                    "count": 3,
                    "type": "FIT",
                    "updatedAt": str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"),
                }
            ],
        },
        ...] ,
    """

    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Загружает информацию о товарах с сайта Casio и обновляет данные товаров для магазинов FBS и DBS в Yandex.Market

    Raises:
        requests.exceptions.ReadTimeout
        requests.exceptions.ConnectionError
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
