import csv
import re
from pymongo import MongoClient



def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile, delimiter=',')
        client = MongoClient()
        test_db = client[db]
        singers_collection = test_db['singers']
        for item in reader:
            int_price = int(item.get('Цена'))
            item.update({'Цена': int_price})
            singers_collection.insert_one(item)


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    client = MongoClient()
    test_db = client[db]
    singers_collection = test_db['singers']
    result = singers_collection.find().sort('Цена', 1)
    for item in result:
        print(item)


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """

    client = MongoClient()
    test_db = client[db]
    singers_collection = test_db['singers']

    # regex = re.compile('укажите регулярное выражение для поиска. ' \
    #                    'Обратите внимание, что в строке могут быть специальные символы, их нужно экранировать')

    regex = re.compile(r'.*' + name + r'.*', re.IGNORECASE)

    result = singers_collection.find({'Исполнитель': {'$regex': regex}}).sort('Цена', 1)
    for item in result:
        print(item)


def delete_collection(db):
    client = MongoClient()
    test_db = client[db]
    singers_collection = test_db['singers']
    result = singers_collection.delete_many({})
    print(result)


if __name__ == '__main__':
    #read_data('artists.csv', 'test')
    #find_cheapest('test')
    #delete_collection('test')
    find_by_name('th', 'test')