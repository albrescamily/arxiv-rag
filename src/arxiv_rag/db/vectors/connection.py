import os

from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


VECTOR_DATABASE_URL = os.environ[
    "VECTOR_DATABASE_URL"
]

vector_engine = create_engine(
    VECTOR_DATABASE_URL,
    pool_pre_ping=True,
)
