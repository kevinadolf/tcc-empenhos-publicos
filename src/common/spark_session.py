"""Utilities to bootstrap and reuse a local Spark session."""

from __future__ import annotations

import atexit
from pathlib import Path
from threading import Lock
from typing import Optional

from pyspark.sql import SparkSession

_SESSION: Optional[SparkSession] = None
_SESSION_LOCK = Lock()


def _register_graphframes(session: SparkSession) -> None:
    """Ensure GraphFrames JARs are available without requiring network access."""
    try:
        import graphframes  # type: ignore
    except ImportError:  # pragma: no cover - handled during optional installs
        return

    lib_dir = Path(graphframes.__file__).resolve().parent / "lib"
    if not lib_dir.exists():  # pragma: no cover - depends on installation layout
        return

    for jar_path in lib_dir.glob("graphframes-*.jar"):
        jar = str(jar_path)
        context = session.sparkContext
        context.addJar(jar)
        context.addPyFile(jar)


def _stop_session() -> None:
    global _SESSION
    if _SESSION is not None:
        _SESSION.stop()
        _SESSION = None


def get_spark_session(app_name: str = "TCCGraphAnalytics") -> SparkSession:
    """Return a cached Spark session configured for local multi-core workloads."""
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    with _SESSION_LOCK:
        if _SESSION is None:
            builder = (
                SparkSession.builder.appName(app_name)
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.driver.host", "127.0.0.1")
            )
            session = builder.getOrCreate()
            session.sparkContext.setLogLevel("WARN")
            _register_graphframes(session)
            atexit.register(_stop_session)
            _SESSION = session
    return _SESSION
