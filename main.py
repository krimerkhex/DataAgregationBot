from pymongo import MongoClient
from bot import main as bot_main
import asyncio
from bson import decode_all
from loger import logger, Loger


def get_data():
    data = None
    with open("dump/sampleDB/sample_collection.bson", 'rb') as file:
        data = decode_all(file.read())
    return data


@Loger
def setup_mongo():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']
    collection = db['mycollection']
    status = collection.insert_many(get_data())
    logger.info("MongoDB push status: ", status)
    client.close()


if __name__ == "__main__":
    # Время заполнения 13 секунд.
    # setup_mongo()
    asyncio.run(bot_main())
