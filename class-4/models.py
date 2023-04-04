from sqlalchemy import Column, Integer, Float, String, ForeignKey, DateTime, Date, Text
from sqlalchemy.dialects.mysql import FLOAT as MY_SQL_FLOAT, JSON
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class CleanLog(Base):
    __tablename__ = "clean-log"

    id = Column(String(255), primary_key=True, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    year = Column(String(255), nullable=False)
    month = Column(String(255), nullable=False)
    day = Column(String(255), nullable=False)
    day_of_week = Column(String(255), nullable=False)
    time = Column(String(255), nullable=False)
    ip = Column(String(255), nullable=False)
    country = Column(String(255), nullable=False)
    city = Column(String(255), nullable=False)
    session = Column(String(255), nullable=False)
    user = Column(String(255), nullable=False)
    is_email = Column(String(255), nullable=False)
    email_domain = Column(String(255), nullable=False)
    rest_method = Column(String(255), nullable=False)
    url = Column(String(255), nullable=False)
    schema = Column(String(255), nullable=False)
    host = Column(String(255), nullable=False)
    rest_session = Column(String(255), nullable=False)
    status = Column(String(255), nullable=False)
    status_verbose = Column(String(255), nullable=False)
    size_bytes = Column(String(255), nullable=False)
    size_kilo_bytes = Column(String(255), nullable=False)
    size_mega_bytes = Column(String(255), nullable=False)


class RowLog(Base):
    __tablename__ = "row-log"

    id = Column(Integer, primary_key=True, autoincrement=True,nullable=False)
    hash_body = Column(String(255), nullable=False)
    timestamp = Column(String(255), nullable=False)
    log = Column(Text, nullable=False)