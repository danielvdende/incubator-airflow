from sqlalchemy import Column, Integer, ForeignKey, String, Text
from sqlalchemy_utc import UtcDateTime
from sqlalchemy.orm import relationship

from airflow.models.utils import Base

class KnownEvent(Base):
    __tablename__ = "known_event"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    user_id = Column(Integer(), ForeignKey('users.id'),)
    known_event_type_id = Column(Integer(), ForeignKey('known_event_type.id'),)
    reported_by = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='known_events')
    event_type = relationship(
        "KnownEventType",
        cascade=False,
        cascade_backrefs=False, backref='known_events')
    description = Column(Text)

    def __repr__(self):
        return self.label
