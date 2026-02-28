from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carrega variáveis de ambiente
load_dotenv()

# Database Connection URL
def database_connection():
    db_url = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
    return create_engine(db_url)