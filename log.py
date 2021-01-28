import logging

logging.basicConfig(level=logging.INFO)

file_handler = logging.FileHandler("./main.log")

logger = logging.getLogger(__name__)
logger.addHandler(file_handler)
logger.propagate = False

logging.getLogger("boto").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
