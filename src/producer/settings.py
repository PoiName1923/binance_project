from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
    KAFKA_PORT = os.getenv("KAFKA_PORT", "9000")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "default-topic")
    SYMBOL_FILE_DIR = os.getenv("SYMBOL_FILE_DIR", "/tmp/symbols.txt")


settings = Settings()