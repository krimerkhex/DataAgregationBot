from bson import decode_all
from datetime import datetime, timedelta
from loger import Loger
import calendar

data: list[dict]


def get_data():
    global data
    with open("dump/sampleDB/sample_collection.bson", 'rb') as file:
        data = decode_all(file.read())


def find_salary(dt, end_dt, group_type: str) -> float:
    global data
    salary = 0
    for item in data:
        match group_type:
            case 'hour':
                if item['dt'].day == dt.day and item['dt'].month == dt.month and item['dt'].year == dt.year and item[
                    'dt'].hour == dt.hour and item['dt'] <= end_dt:
                    salary += item['value']
                    if dt.day == 2:
                        print(item['value'], item['dt'])
            case 'day':
                if item['dt'].day == dt.day and item['dt'].month == dt.month and item['dt'].year == dt.year and item['dt'] <= end_dt:
                    salary += item['value']
            case 'month':
                if item['dt'].month == dt.month and item['dt'].year == dt.year and item['dt'] <= end_dt:
                    salary += item['value']
    return salary


def change_dt(dt, group_type: str):
    match group_type:
        case 'hour':
            return dt + timedelta(hours=1)
        case 'day':
            return dt + timedelta(days=1)
        case 'month':
            return dt + timedelta(days=calendar.monthrange(dt.year, dt.month)[1])


@Loger
def salary_aggregation(dt_from: str, dt_upto: str, group_type: str) -> dict[str: list]:
    get_data()
    global data
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
    print(salary_aggregation("2022-02-01T00:00:00", "2022-02-02T00:00:00", "hour"))
