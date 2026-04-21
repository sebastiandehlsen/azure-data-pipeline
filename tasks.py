import time
from logger import get_logger

logger = get_logger()


class Task:
    def __init__(self, name, func, retries=0):
        self.name = name
        self.func = func
        self.retries = retries

    def run(self):
        attempt = 0

        while attempt <= self.retries:
            try:
                logger.info(f"Starting task: {self.name} (attempt {attempt + 1})")
                start = time.time()

                self.func()

                duration = time.time() - start
                logger.info(f"Finished task: {self.name} in {duration:.2f}s")
                return

            except Exception as e:
                attempt += 1
                logger.error(f"Task failed: {self.name} | Error: {e}")

                if attempt > self.retries:
                    logger.error(f"Task permanently failed: {self.name}")
                    raise

                logger.info(f"Retrying task: {self.name}")