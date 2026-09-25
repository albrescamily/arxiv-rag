import os

from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


METADATA_DATABASE_URL = os.environ[
    "METADATA_DATABASE_URL"
]

metadata_engine = create_engine(
    METADATA_DATABASE_URL,
    pool_pre_ping=True,
)
