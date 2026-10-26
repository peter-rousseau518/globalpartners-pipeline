import importlib.util
import os
import sys

import pytest
from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_job(name):
    """Import a Glue job script as a module (its AWS-only imports live inside main())."""
    path = os.path.join(ROOT, "glue_jobs", f"{name}.py")
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("gp-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def silver():
    return load_job("silver_transform")


@pytest.fixture(scope="session")
def gold():
    return load_job("gold_model")


@pytest.fixture(scope="session")
def metrics():
    return load_job("gold_metrics")
