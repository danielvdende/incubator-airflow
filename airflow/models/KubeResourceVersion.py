from sqlalchemy import (
    Column, String, Boolean
)
from sqlalchemy import true as sqltrue

from airflow.utils.db import provide_session

from airflow.models.utils import Base


class KubeResourceVersion(Base):
    __tablename__ = "kube_resource_version"
    one_row_id = Column(Boolean, server_default=sqltrue(), primary_key=True)
    resource_version = Column(String(255))

    @staticmethod
    @provide_session
    def get_current_resource_version(session=None):
        (resource_version,) = session.query(KubeResourceVersion.resource_version).one()
        return resource_version

    @staticmethod
    @provide_session
    def checkpoint_resource_version(resource_version, session=None):
        if resource_version:
            session.query(KubeResourceVersion).update({
                KubeResourceVersion.resource_version: resource_version
            })
            session.commit()

    @staticmethod
    @provide_session
    def reset_resource_version(session=None):
        session.query(KubeResourceVersion).update({
            KubeResourceVersion.resource_version: '0'
        })
        session.commit()
        return '0'
