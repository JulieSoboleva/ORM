import os
import json
import sqlalchemy as sq
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate
from models import create_tables, Publisher, Shop, Book, Stock, Sale

LOGIN = os.getenv('PG_LOGIN')
PASSWORD = os.getenv('PG_PASSWORD')
DATABASE = os.getenv('PG_DATABASE')
HOST = os.getenv('PG_HOST')
PORT = os.getenv('PG_PORT')
DSN = f'postgresql://{LOGIN}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'


def fill_tables(session):
    with open('fixtures/tests_data.json', 'r') as file:
        data = json.load(file)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
        session.commit()


def execute_request(session, publisher):
    try:
        publisher_id = int(publisher)
    except ValueError:
        stmt = select(Publisher).where(Publisher.name == publisher)
        result = session.execute(stmt).first()
        if result is None:
            print('Издательство с заданным названием не найдено.')
            return
        publisher_id = result.Publisher.id

    stmt = (select(Book, Shop, Sale).join(Publisher, Book.publisher)
            .join(Stock, Stock.id_book == Book.id)
            .join(Shop, Shop.id == Stock.id_shop)
            .join(Sale, Stock.id == Sale.id_stock, isouter=True)
            .where(Publisher.id == publisher_id))

    data = []
    for row in session.execute(stmt):
        data.append([row.Book.title, row.Shop.name,
                     row.Sale.price if row.Sale is not None else 'NULL',
                     row.Sale.date_sale if row.Sale is not None else 'NULL'])
    headers = ['Название книги', 'Название магазина',
               'Стоимость покупки', 'Дата покупки']
    print(tabulate(data, headers, tablefmt='outline'))


if __name__ == '__main__':
    engine = sq.create_engine(DSN)
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    fill_tables(session)
    execute_request(session, input('Введите название или id издательства: '))
