from bot import main as bot_main
import asyncio
from setup_mongo import setup_mongo

if __name__ == "__main__":
    # Время заполнения 13 секунд.
    # setup_mongo()
    asyncio.run(bot_main())
