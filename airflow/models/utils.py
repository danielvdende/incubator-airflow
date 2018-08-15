from future.standard_library import install_aliases
from sqlalchemy.ext.declarative import declarative_base

from airflow import configuration
from airflow import settings
from airflow.models.DagRun import DagRun
from airflow.models.NullFernet import NullFernet
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from builtins import ImportError as BuiltinImportError

install_aliases()

Base = declarative_base()
ID_LEN = 250
XCOM_RETURN_KEY = 'return_value'

Stats = settings.Stats


_fernet = None


def get_fernet():
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: AirflowException if there's a problem trying to load Fernet
    """
    global _fernet
    if _fernet:
        return _fernet
    try:
        from cryptography.fernet import Fernet, InvalidToken
        global InvalidFernetToken
        InvalidFernetToken = InvalidToken

    except BuiltinImportError:
        LoggingMixin().log.warn("cryptography not found - values will not be stored "
                                "encrypted.",
                                exc_info=1)
        _fernet = NullFernet()
        return _fernet

    try:
        _fernet = Fernet(configuration.conf.get('core', 'FERNET_KEY').encode('utf-8'))
        _fernet.is_encrypted = True
        return _fernet
    except (ValueError, TypeError) as ve:
        raise AirflowException("Could not create Fernet object: {}".format(ve))


_CONTEXT_MANAGER_DAG = None


def clear_task_instances(tis,
                         session,
                         activate_dag_runs=True,
                         dag=None,
                         ):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param activate_dag_runs: flag to check for active dag run
    :param dag: DAG object
    """
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the current task try number.
                ti.max_tries = max(ti.max_tries, ti.try_number - 1)
            ti.state = State.NONE
            session.merge(ti)

    if job_ids:
        from airflow.jobs import BaseJob as BJ
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN

    if activate_dag_runs and tis:
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_({ti.dag_id for ti in tis}),
            DagRun.execution_date.in_({ti.execution_date for ti in tis}),
        ).all()
        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = timezone.utcnow()
