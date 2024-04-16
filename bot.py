import json

from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

import logging
from agregator import salary_aggregation
from temp import Token
import asyncio
import sys

dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")


@dp.message()
async def echo_handler(message: Message) -> None:
    try:
        temp = json.loads(message.text)
        answer = str(salary_aggregation(temp['dt_from'], temp['dt_upto'], temp['group_type'])).replace("\'", "\"")
        print(answer)
        await message.answer(text=answer)
    except TypeError:
        await message.answer("Nice try!")


async def main() -> None:
    bot = Bot(token=Token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
