from sqlalchemy import (
    Column, Boolean, String
)
from sqlalchemy import true as sqltrue

import uuid

from airflow.utils.db import provide_session
from airflow.models.utils import Base


class KubeWorkerIdentifier(Base):
    __tablename__ = "kube_worker_uuid"
    one_row_id = Column(Boolean, server_default=sqltrue(), primary_key=True)
    worker_uuid = Column(String(255))

    @staticmethod
    @provide_session
    def get_or_create_current_kube_worker_uuid(session=None):
        (worker_uuid,) = session.query(KubeWorkerIdentifier.worker_uuid).one()
        if worker_uuid == '':
            worker_uuid = str(uuid.uuid4())
            KubeWorkerIdentifier.checkpoint_kube_worker_uuid(worker_uuid, session)
        return worker_uuid

    @staticmethod
    @provide_session
    def checkpoint_kube_worker_uuid(worker_uuid, session=None):
        if worker_uuid:
            session.query(KubeWorkerIdentifier).update({
                KubeWorkerIdentifier.worker_uuid: worker_uuid
            })
            session.commit()
