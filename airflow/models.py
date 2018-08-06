# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future.standard_library import install_aliases

from builtins import str, object, bytes, ImportError as BuiltinImportError
import copy
from collections import namedtuple, defaultdict
from datetime import timedelta

import dill
import functools
import getpass
import imp
import importlib
import itertools
import zipfile
import jinja2
import json
import logging
import numbers
import os
import pickle
import re
import signal
import sys
import textwrap
import traceback
import warnings
import hashlib

import uuid
from datetime import datetime
from urllib.parse import urlparse, quote, parse_qsl

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, LargeBinary, UniqueConstraint)
from sqlalchemy import func, or_, and_, true as sqltrue
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import reconstructor, relationship, synonym
from sqlalchemy_utc import UtcDateTime

from croniter import croniter
import six

from airflow import settings, utils
from airflow.executors import GetDefaultExecutor, LocalExecutor
from airflow import configuration
from airflow.exceptions import (
    AirflowDagCycleException, AirflowException, AirflowSkipException, AirflowTaskTimeout
)
from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep

from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from airflow.utils import timezone
from airflow.utils.dag_processing import list_py_file_paths
from airflow.utils.dates import cron_presets, date_range as utils_date_range
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.email import send_email
from airflow.utils.helpers import (
    as_tuple, is_container, validate_key, pprinttable)
from airflow.utils.operator_resources import Resources
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule
from airflow.utils.net import get_hostname
from airflow.utils.log.logging_mixin import LoggingMixin

install_aliases()

Base = declarative_base()
ID_LEN = 250
XCOM_RETURN_KEY = 'return_value'

Stats = settings.Stats


# DANIEL: this variable is only used in the function below. Probably need to be brought together to work
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


# Used by DAG context_managers
# DANIEL: used across modules.
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























