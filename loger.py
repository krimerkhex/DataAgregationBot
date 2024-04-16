from time import perf_counter
from loguru import logger
from functools import wraps


def Loger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"{func.__name__} started with arguments: {args}")
        if not len(kwargs.keys()):
            logger.info(f"keyword arguments:")
            for key, value in kwargs.items():
                logger.info(f"{key} : {value}")
        start_time = perf_counter()
        result = func(*args, **kwargs)
        logger.info(f"{func.__name__} end's. result value: {result}")
        logger.info(f"{func.__name__} time took {(perf_counter() - start_time):.4f} ms.\n\n")
        return result

    return wrapper
