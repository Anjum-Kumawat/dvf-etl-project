"""Regression test for the real 2026-10-01 schedule tick failure: the schedule
yielded one RunRequest per department without a run_key, which Dagster
rejects, so the tick launched 0 runs.
"""
import datetime

from dagster import build_schedule_context

from orchestration.dagster.dvf_dagster.assets import DEPARTMENTS, dvf_publication_schedule
from orchestration.dagster.dvf_dagster.definitions import defs


def _evaluate(scheduled_time):
    # repository_def is needed to resolve partition_key against the job, as
    # the daemon does on a real tick.
    context = build_schedule_context(
        scheduled_execution_time=scheduled_time,
        repository_def=defs.get_repository_def(),
    )
    # evaluate_tick runs the same validation the daemon does on a real tick.
    return dvf_publication_schedule.evaluate_tick(context)


def test_october_tick_requests_one_run_per_department():
    result = _evaluate(datetime.datetime(2026, 10, 1, 6, 0, tzinfo=datetime.timezone.utc))

    assert [r.partition_key for r in result.run_requests] == list(DEPARTMENTS)
    assert [r.run_key for r in result.run_requests] == [f"2026-10-{d}" for d in DEPARTMENTS]


def test_run_keys_differ_between_publications():
    april = _evaluate(datetime.datetime(2027, 4, 1, 6, 0, tzinfo=datetime.timezone.utc))
    october = _evaluate(datetime.datetime(2026, 10, 1, 6, 0, tzinfo=datetime.timezone.utc))

    april_keys = {r.run_key for r in april.run_requests}
    october_keys = {r.run_key for r in october.run_requests}
    assert april_keys.isdisjoint(october_keys)
