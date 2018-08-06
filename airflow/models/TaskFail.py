from sqlalchemy import Column, Integer, String, Index
from sqlalchemy_utc import UtcDateTime

class TaskFail(Base):
    """
    TaskFail tracks the failed run durations of each task instance.
    """

    __tablename__ = "task_fail"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)
    execution_date = Column(UtcDateTime, nullable=False)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Integer)

    __table_args__ = (
        Index('idx_task_fail_dag_task_date', dag_id, task_id, execution_date,
              unique=False),
    )

    def __init__(self, task, execution_date, start_date, end_date):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.end_date = end_date
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None
