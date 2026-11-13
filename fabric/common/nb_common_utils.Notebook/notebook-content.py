# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a05dbecc-1c93-44b9-8c73-8b6bff7026b1",
# META       "default_lakehouse_name": "AwesomeLH_dev",
# META       "default_lakehouse_workspace_id": "e1127f86-76fc-45a2-8c21-350a41e41a8d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a05dbecc-1c93-44b9-8c73-8b6bff7026b1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_common_utils
# Shared helpers for the layer notebooks. Load with `%run nb_common_utils` **after** the parameters cell.
# **`RunLogger`**: wraps each unit of work in a *step*, captures errors without stopping the notebook mid-way, then persists logs before deciding the outcome:
# | Where | Grain |
# |---|---|
# | `ops.notebook_runs` | 1 row per notebook run: parameters, status, step counts, duration, error + traceback |
# | `ops.notebook_steps` | 1 row per step: target table, status, rows, details (JSON), duration, error + traceback |
# | `Files/logs/<notebook>/date=YYYY-MM-DD/<run_id>.log` | full text log |
# ```python
# log = RunLogger("nb_x", run_id=run_id, pipeline_name=pipeline_name, parameters={...})
# log.step("build table", "CREATE OR REPLACE TABLE ... AS SELECT ...", target="schema.table")
# log.step("custom", lambda: {"rows": 10, "anything": "json-serialisable"})
# log.finish()   # persist logs
# log.exit()     # notebook exit value (JSON) or raise -> pipeline activity fails
# ```
# After a failed step, later steps are recorded as `skipped` (`stop_on_error=True`).


# CELL ********************

import io
import json
import logging
import re
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_bool(value) -> bool:
    """Pipelines often pass booleans as strings ("true"/"false")."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def table_exists(name: str) -> bool:
    try:
        spark.table(name).limit(0)
        return True
    except Exception:
        return False


def sql_literal(value: str) -> str:
    """Quote a Python string as a Spark SQL string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


class RunLogger:
    STEP_STATUSES = ("succeeded", "failed", "skipped")

    def __init__(self, notebook, run_id="", pipeline_name="", parameters=None,
                 log_schema="ops", log_folder="Files/logs", stop_on_error=True):
        self.notebook = notebook
        self.run_id = str(run_id).strip() or str(uuid.uuid4())
        self.pipeline_name = str(pipeline_name).strip() or "manual"
        self.parameters = parameters or {}
        self.log_schema = log_schema
        self.stop_on_error = stop_on_error
        self.started_at = utc_now()
        self.ended_at = None
        self.steps = []
        self.run_error = None
        self.summary = None
        self.log_file = (f"{log_folder.rstrip('/')}/{notebook}/"
                         f"date={self.started_at:%Y-%m-%d}/{self.run_id}.log")

        self._buffer = io.StringIO()
        self.logger = logging.getLogger(f"{notebook}.{self.run_id}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        for h in list(self.logger.handlers):
            self.logger.removeHandler(h)
        fmt = logging.Formatter(f"%(asctime)s | %(levelname)-7s | run={self.run_id[:8]} | %(message)s",
                                datefmt="%Y-%m-%dT%H:%M:%SZ")
        fmt.converter = time.gmtime
        for handler in (logging.StreamHandler(sys.stdout), logging.StreamHandler(self._buffer)):
            handler.setFormatter(fmt)
            self.logger.addHandler(handler)
        self.logger.info(f"Starting {notebook} (pipeline={self.pipeline_name})")
        if self.parameters:
            self.logger.info(f"Parameters: {json.dumps(self.parameters, default=str)}")

    # -- logging shortcuts -------------------------------------------------
    def info(self, msg):    self.logger.info(msg)
    def warning(self, msg): self.logger.warning(msg)
    def error(self, msg):   self.logger.error(msg)

    @property
    def failed(self) -> bool:
        return self.run_error is not None or any(s["status"] == "failed" for s in self.steps)

    # -- steps --------------------------------------------------------------
    def step(self, name, action, target=None, details=None):
        """Run one unit of work. `action` is a Spark SQL string or a callable.
        A callable may return a dict (stored as details; key "rows" is lifted)
        or an int (rows). If `target` is given and rows are unknown, rows = count(target).
        Returns the action's result, or None if the step failed/was skipped."""
        seq = len(self.steps) + 1
        record = {"step_seq": seq, "step_name": name, "target": target, "status": None,
                  "rows": None, "details": dict(details or {}), "error_message": None,
                  "error_traceback": None, "started_at": utc_now()}
        if self.failed and self.stop_on_error:
            record.update(status="skipped", ended_at=utc_now(), duration_s=0.0)
            self.steps.append(record)
            self.logger.warning(f"[{seq:02d}] {name}: skipped (earlier failure)")
            return None

        self.logger.info(f"[{seq:02d}] {name} ...")
        result = None
        try:
            result = spark.sql(action) if isinstance(action, str) else action()
            if isinstance(result, dict):
                record["details"].update({k: v for k, v in result.items() if k != "rows"})
                record["rows"] = result.get("rows")
            elif isinstance(result, int) and not isinstance(result, bool):
                record["rows"] = result
            if record["rows"] is None and target:
                record["rows"] = spark.table(target).count()
            record["status"] = "succeeded"
            rows_txt = f" ({record['rows']:,} rows)" if record["rows"] is not None else ""
            self.logger.info(f"[{seq:02d}] {name}: succeeded{rows_txt}")
        except Exception as e:
            record["status"] = "failed"
            record["error_message"] = f"{type(e).__name__}: {str(e)[:4000]}"
            record["error_traceback"] = traceback.format_exc()
            self.logger.error(f"[{seq:02d}] {name}: FAILED - {record['error_message'][:500]}")
            result = None
        finally:
            record["ended_at"] = utc_now()
            record["duration_s"] = round((record["ended_at"] - record["started_at"]).total_seconds(), 2)
            self.steps.append(record)
        return result

    def fail_run(self, exc: Exception, context: str = "Run failed"):
        """Record a run-level error (e.g. invalid parameters)."""
        self.run_error = {"message": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc()}
        self.logger.error(f"{context}: {self.run_error['message']}")

    # -- finish -------------------------------------------------------------
    def _status(self):
        return "failed" if self.failed else "succeeded"

    def _error_message(self):
        if self.run_error:
            return self.run_error["message"]
        failed = [s for s in self.steps if s["status"] == "failed"]
        return "; ".join(f"{s['step_name']}: {s['error_message']}" for s in failed)[:4000] or None

    def finish(self):
        """Persist run + step logs. Never raises."""
        self.ended_at = utc_now()
        counts = {st: sum(s["status"] == st for s in self.steps) for st in self.STEP_STATUSES}
        status = self._status()
        self.summary = {
            "notebook": self.notebook, "run_id": self.run_id, "pipeline_name": self.pipeline_name,
            "status": status, "steps": len(self.steps), **{f"steps_{k}": v for k, v in counts.items()},
            "duration_s": round((self.ended_at - self.started_at).total_seconds(), 2),
            "log_file": self.log_file, "error": self._error_message(),
        }
        self.logger.info(f"Run {status}: " + ", ".join(f"{k}={v}" for k, v in counts.items()))
        try:
            self._persist_tables(counts, status)
            self.logger.info(f"Logs written to {self.log_schema}.notebook_runs / {self.log_schema}.notebook_steps")
        except Exception as e:
            self.logger.error(f"Could not write log tables: {type(e).__name__}: {e}")
        try:
            notebookutils.fs.mkdirs(self.log_file.rsplit("/", 1)[0])
            notebookutils.fs.put(self.log_file, self._buffer.getvalue(), True)
            print(f"Log file: {self.log_file}")
        except Exception as e:
            print(f"Could not write log file {self.log_file}: {type(e).__name__}: {e}")
        return self.summary

    def _persist_tables(self, counts, status):
        from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                                       StructField, StructType, TimestampType)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.log_schema}")

        runs_schema = StructType([
            StructField("run_id", StringType()), StructField("notebook", StringType()),
            StructField("pipeline_name", StringType()), StructField("status", StringType()),
            StructField("parameters", StringType()), StructField("steps_total", IntegerType()),
            StructField("steps_succeeded", IntegerType()), StructField("steps_failed", IntegerType()),
            StructField("steps_skipped", IntegerType()), StructField("started_at", TimestampType()),
            StructField("ended_at", TimestampType()), StructField("duration_s", DoubleType()),
            StructField("log_file", StringType()), StructField("error_message", StringType()),
            StructField("error_traceback", StringType()),
        ])
        runs_row = [(
            self.run_id, self.notebook, self.pipeline_name, status,
            json.dumps(self.parameters, default=str), len(self.steps),
            counts["succeeded"], counts["failed"], counts["skipped"],
            self.started_at, self.ended_at, float(self.summary["duration_s"]), self.log_file,
            self._error_message(), self.run_error["traceback"] if self.run_error else None,
        )]
        (spark.createDataFrame(runs_row, runs_schema).write.mode("append")
              .option("mergeSchema", "true").saveAsTable(f"{self.log_schema}.notebook_runs"))

        if not self.steps:
            return
        steps_schema = StructType([
            StructField("run_id", StringType()), StructField("notebook", StringType()),
            StructField("step_seq", IntegerType()), StructField("step_name", StringType()),
            StructField("target", StringType()), StructField("status", StringType()),
            StructField("rows", LongType()), StructField("details", StringType()),
            StructField("started_at", TimestampType()), StructField("ended_at", TimestampType()),
            StructField("duration_s", DoubleType()), StructField("error_message", StringType()),
            StructField("error_traceback", StringType()),
        ])
        rows = [(
            self.run_id, self.notebook, s["step_seq"], s["step_name"], s["target"], s["status"],
            s["rows"], json.dumps(s["details"], default=str) if s["details"] else None,
            s["started_at"], s["ended_at"], float(s["duration_s"]), s["error_message"], s["error_traceback"],
        ) for s in self.steps]
        (spark.createDataFrame(rows, steps_schema).write.mode("append")
              .option("mergeSchema", "true").saveAsTable(f"{self.log_schema}.notebook_steps"))

    def exit(self):
        """Hand the outcome to the orchestrator: raise on failure, else exit with JSON."""
        summary = self.summary or self.finish()
        print(json.dumps(summary, indent=2, default=str))
        if summary["status"] == "failed":
            raise RuntimeError(f"{self.notebook} failed: {json.dumps(summary, default=str)}")
        notebookutils.notebook.exit(json.dumps(summary, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
