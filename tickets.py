import csv
import re
from pymongo import MongoClient, ASCENDING
from datetime import datetime


def read_data(csv_file, database):
    """
    (string, database link) -> None

    Function sorts tickets by cost

    """

    artists_collection = database['artists']

    artists_collection.delete_many({})

    with open(csv_file, encoding='utf8') as csv_file:
        reader = csv.DictReader(csv_file)
        for line in reader:
            art = {}
            for key, value in line.items():
                if key == 'Цена':
                    art[key] = int(value)
                elif key == 'Дата':
                    art[key] = datetime.strptime(value + '.2020', '%d.%m.%Y')
                else:
                    art[key] = value
            artists_collection.insert_one(art)


def find_cheapest(rows):
    """
    (collection) -> collection

    Function sorts tickets by cost

    """

    sorted_rows = rows.sort('Цена', ASCENDING)
    return sorted_rows


def find_earliest(rows):
    """
    (collection) -> collection

    Function sorts tickets by date

    """

    sorted_rows = rows.sort('Дата', ASCENDING)
    return sorted_rows


def find_by_name(name, artists_collection):
    """
    (string, collection) -> collection

    Function finds tickets by name of artist even if you ask part of his name and return string of cheapest ticket

    """

    all_artists = artists_collection.find()
    if all_artists is None:
        return
    pattern = f"^[а-яА-Яa-zA-Z ]*{name}[а[а-яА-Яa-zA-Z ]*"
    regex = re.compile(pattern, re.I)
    found_rows = []
    for item in all_artists:
        row_list = list(item.values())[1::1]
        s = ','.join(str(e) for e in row_list)
        m = regex.search(s)
        if m is not None:
            found_rows.append(m.group(0))

    if found_rows is not None:
        need_artists = artists_collection.find({'Исполнитель': {'$in': found_rows}})
        # print_find(artists, {'Исполнитель': {'$in': found}})
        return find_cheapest(need_artists)


def print_find(cursor):
    """
        (cursor) -> None

        Function prints data of reply on request to db

        """
    for number, item in enumerate(cursor, 1):
        print(f"{number}. ", end="")
        for row in list(item.items())[1::1]:
            if row[0] == 'Дата':
                row_string = row[1].strftime("%d.%m.%Y")
            else:
                row_string = row[1]
            print(f"{row[0]}: {row_string}; ", end="")
        print("")


if __name__ == '__main__':
    client = MongoClient()
    db = client['artists_db']
    read_data('artists.csv', db)
    artists = db['artists']

    print(f"Список в порядке возрастания цены:")
    print_find(find_cheapest(artists.find()))
    print("")

    print(f"Список в порядке возрастания даты:")
    print_find(find_earliest(artists.find()))
    print("")

    while True:
        find_string = input("Введите строку поиска по исполнителю(команда: \"quit\" - выход): ")
        find_string_crop = find_string.strip()
        if len(find_string_crop) <= 0:
            continue
        elif find_string_crop == 'quit':
            print("Выход из программы.")
            break
        else:
            found = find_by_name(find_string, artists)
            if found is None:
                print("Ничего не найдено.")
            else:
                print_find(found)






