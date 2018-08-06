from sqlalchemy import (
    Column, Integer, String, Text
)
from sqlalchemy_utc import UtcDateTime


class ImportError(Base):
    __tablename__ = "import_error"
    id = Column(Integer, primary_key=True)
    timestamp = Column(UtcDateTime)
    filename = Column(String(1024))
    stacktrace = Column(Text)
