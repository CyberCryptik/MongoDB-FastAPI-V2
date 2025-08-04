import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv  # to load env variables

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")

client = AsyncIOMotorClient(MONGODB_URI)
db = client[DB_NAME]


