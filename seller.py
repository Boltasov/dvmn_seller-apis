import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров нашего магазина в Озон по API.

    Arguments:
        last_id (str): Идентификатор последнего товара, полученного в ответе API Ozon.
        client_id (str): Идентификатор клиента, сгенерированный для доступа к API Ozon.
         (str): Токен, сгенерированный для доступа к API Ozon.

    Returns:
        dict: Содержимое ответа API со списком товаров.

    Examples:
        >>> env = Env()
        >>> get_product_list("", env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        [...]

        >>> get_product_list("15", env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        [...]

    Raises:
        requests.exceptions.HTTPError: Если CLIENT_ID и SELLER_TOKEN не указаны или не актуальны.
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров нашего магазина в Озон

    Arguments:
        client_id (str): Идентификатор клиента, сгенерированный для доступа к API Ozon.
        seller_token (str): Токен, сгенерированный для доступа к API Ozon.

    Returns:
        list: Список артикулов товаров нашего магазина в Ozon.

    Examples:
        >>> env = Env()
        >>> get_offer_ids(env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        [...]

    Raises:
        requests.exceptions.HTTPError: Если CLIENT_ID и SELLER_TOKEN не указаны или не актуальны.
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров в магазине.

    Arguments:
        prices: Список словарей, содержащих данные об артикуле и его цене.
        client_id (str): Идентификатор клиента, сгенерированный для доступа к API Ozon.
        seller_token (str): Токен, сгенерированный для доступа к API Ozon.

    Returns:
        dict: Содержание ответа API Ozon

    Examples:
        >>> env = Env()
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> some_price = list(divide(prices, 900))[0]
        >>> update_price(some_price, env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        {...}

    Raises:
        requests.exceptions.HTTPError: Если CLIENT_ID и SELLER_TOKEN не указаны или не актуальны.

    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить информацию о товарах на основе данных об остатках.

    Arguments:
        stocks: Данные об остатках товаров.
        client_id (str): Идентификатор клиента, сгенерированный для доступа к API Ozon.
        seller_token (str): Токен, сгенерированный для доступа к API Ozon.

    Returns:
        dict: Содержание ответа API Ozon

    Examples:
        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> some_stock = list(divide(stocks, 100))[0]
        >>> update_stocks(some_stock, env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        {...}

    Raises:
        requests.exceptions.HTTPError: Если CLIENT_ID и SELLER_TOKEN не указаны или не актуальны.
    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл с доступными товарами с сайта Casio

    Returns:
        dict: Словарь с данными о доступных для перепродажи товарах.

    Examples:
        >>> download_stock()
        {}
    """

    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Подготовить данные товаров для загрузки в магазин Ozon

    Arguments:
        watch_remnants (dict): Словарь с данными о товарах.
        offer_ids (list): Список имеющихся в магазине товаров.

    Returns:
        list: Список обновлённых данных о товарах для магазина Ozon.

    Examples:
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        >>> watch_remnants = download_stock()
        >>> create_stocks(watch_remnants, offer_ids)
        []

    Raises:
        requests.exceptions.HTTPError: Если CLIENT_ID и SELLER_TOKEN не указаны или не актуальны.
    """

    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Обновить цены для товаров.

    Arguments:
        watch_remnants (dict): Словарь с данными о товарах.
        offer_ids (list): Список имеющихся в магазине товаров.

    Returns:
        list: Список товаров с обновлёнными ценами.

    Examples:
        >>> offer_ids = get_offer_ids(client_id, seller_token)
        >>> watch_remnants = download_stock()
        >>> create_prices(watch_remnants, offer_ids)
        [...]
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать строку цены с дополнительными символами в строку с целой частью цены.

    Args:
        price: Строка, содержащая цену на товар.

    Returns:
        Возвращает строку из цифр, обозначающую целочисленное значение введённой цены.

    Examples:
        Корректное использование функции.

        >>> price_conversion("5'990.00 руб.")
        '5990'

        Некорректное использование. Целая часть должна быть отделена от дробной точкой.

        >>> price_conversion("5'990,00 руб.")
        '599000'
    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов

    Arguments:
        lst: Список, который нужно разделить на части
        n: Количество частей, на которые нужно разделить список

    Yields:
        list: N-я часть списка

    Examples:
        >>> lst = [1,2,3,4]
        >>> for i in divide(lst, 2):
        >>>     print(i)
        [1,2]
        [3,4]
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить новые цены товаров в магазин Ozon.

    Arguments:
        watch_remnants (dict): Словарь с данными о товарах.
        client_id (str): Идентификатор клиента, сгенерированный для доступа к API Ozon.
        seller_token (str): Токен, сгенерированный для доступа к API Ozon.

    Returns:
        list: Цены, которые были загружены.

    Examples:
        >>> env = Env()
        >>> watch_remnants = download_stock()
        >>> upload_prices(watch_remnants, env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        [...]
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновить данные товарах в магазине Ozon.

        Arguments:
            watch_remnants (dict): Словарь с данными о товарах.
            client_id (str): Идентификатор клиента, сгенерированный для доступа к API Ozon.
            seller_token (str): Токен, сгенерированный для доступа к API Ozon.

        Returns:
            list: Список товаров, которые ещё остались в продаже.
            list: Список данных о товарах, которые были обновлены.

        Examples:
            >>> env = Env()
            >>> watch_remnants = download_stock()
            >>> upload_stocks(watch_remnants, env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
            [...]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Загружает информацию о товарах с сайта Casio и обновляет данные товаров для магазина в Ozon

    Raises:
        requests.exceptions.ReadTimeout
        requests.exceptions.ConnectionError
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
