from datetime import datetime, timedelta
from loger import Loger
from calendar import monthrange
from pymongo import MongoClient


def find_salary(dt, end_dt, group_type: str) -> float:
    salary = 0
    client = MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']
    collection = db['mycollection']
    query = {}
    match group_type:
        case 'hour':
            query['dt'] = {'$gte': dt, '$lt': dt + timedelta(hours=1), '$lte': end_dt}
        case 'day':
            query['dt'] = {'$gte': dt, '$lt': dt + timedelta(days=1), '$lte': end_dt}
        case 'month':  # Один баг здесь. Приходить 5516474, должно быть 5515874. Входные данные 1.
            query['dt'] = {'$gte': dt, '$lte': dt + timedelta(days=monthrange(dt.year, dt.month)[1])}
    pipeline = [
        {'$match': query},
        {'$group': {'_id': None, 'total': {'$sum': '$value'}}}
    ]
    result = collection.aggregate(pipeline)
    for doc in result:
        salary += doc['total']
    client.close()
    return salary


def change_dt(dt, group_type: str):
    match group_type:
        case 'hour':
            return dt + timedelta(hours=1)
        case 'day':
            return dt + timedelta(days=1)
        case 'month':
            return dt + timedelta(days=monthrange(dt.year, dt.month)[1])


@Loger
def salary_aggregation(dt_from: str, dt_upto: str, group_type: str) -> dict[str: list]:
    dataset = []
    labels = []
    dt_from = datetime.strptime(dt_from, "%Y-%m-%dT%H:%M:%S")
    dt_upto = datetime.strptime(dt_upto, "%Y-%m-%dT%H:%M:%S")
    while dt_from <= dt_upto:
        match group_type:
            case 'hour':
                labels.append(str(dt_from).replace(" ", "T"))
                dataset.append(find_salary(dt_from, dt_upto, group_type))
            case 'day':
                labels.append(str(dt_from).replace(" ", "T"))
                dataset.append(find_salary(dt_from, dt_upto, group_type))
            case 'month':
                labels.append(str(dt_from).replace(" ", "T"))
                dataset.append(find_salary(dt_from, dt_upto, group_type))
            case _:
                raise Exception("Invalid group_type")
        dt_from = change_dt(dt_from, group_type)
    response = {"dataset": dataset, "labels": labels}
    return response


if __name__ == "__main__":
    print(salary_aggregation("2022-09-01T00:00:00", "2022-12-31T23:59:00", "month"))
