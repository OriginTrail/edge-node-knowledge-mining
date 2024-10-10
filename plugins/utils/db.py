from sqlalchemy import create_engine, Column, Integer, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
import os


DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# Create SQLAlchemy engine using environment variables
DATABASE_URL = f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


# Create a session factory
Session = sessionmaker(bind=engine)

Base = declarative_base()


# Define a logging model
class LogEntry(Base):
    __tablename__ = "log_entries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    file_size = Column(Integer, nullable=False)
    response_time = Column(Float, nullable=False)
    ka_json_ld = Column(Text, nullable=False)
    error_flag = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)


# Create the table
Base.metadata.create_all(engine)
