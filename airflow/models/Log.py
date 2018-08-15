from sqlalchemy import (
    Column, Integer, String, Text
)
from sqlalchemy_utc import UtcDateTime

from airflow.models.utils import Base, ID_LEN
from airflow.utils import timezone


class Log(Base):
    """
    Used to actively log events to the database
    """

    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    dttm = Column(UtcDateTime)
    dag_id = Column(String(ID_LEN))
    task_id = Column(String(ID_LEN))
    event = Column(String(30))
    execution_date = Column(UtcDateTime)
    owner = Column(String(500))
    extra = Column(Text)

    def __init__(self, event, task_instance, owner=None, extra=None, **kwargs):
        self.dttm = timezone.utcnow()
        self.event = event
        self.extra = extra

        task_owner = None

        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.execution_date = task_instance.execution_date
            task_owner = task_instance.task.owner

        if 'task_id' in kwargs:
            self.task_id = kwargs['task_id']
        if 'dag_id' in kwargs:
            self.dag_id = kwargs['dag_id']
        if 'execution_date' in kwargs:
            if kwargs['execution_date']:
                self.execution_date = kwargs['execution_date']

        self.owner = owner or task_owner
