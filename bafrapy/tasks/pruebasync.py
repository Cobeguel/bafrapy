# create sqlalchemy session
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bafrapy.logger import LoguruConfig, LoguruLogger
from bafrapy.providers.binance import BinanceFactory
from bafrapy.repositories import MainRepository
from bafrapy.tasks.syncsymbols import SyncSymbolsPayload, SyncSymbolsTask

# Load environment variables from .env file
load_dotenv()

# Get database connection details from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_PORT = os.getenv("DB_EXTERNAL_PORT", "3307")  # Using external port with default fallback
DB_HOST = os.getenv("DB_HOST", "localhost")  # Default to localhost if not specified

# Create database URL
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

log_config = LoguruConfig.from_file("bafrapy/config/log.json")
log = LoguruLogger()
log.set_config(log_config)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, expire_on_commit=False)

repo = MainRepository(SessionLocal)

factory = BinanceFactory.from_config_file("bafrapy/config/providers/binance.json")

provider = factory.create_provider()

payload = SyncSymbolsPayload(provider="BINANCE", batch_size=1)

task = SyncSymbolsTask(data=payload, repository=repo, provider=provider)

task.run()