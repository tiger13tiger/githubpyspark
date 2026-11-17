from pyspark.sql import functions as F

from tests.conftest import impl

ex = impl("ex05_nested")


def test_load_events(spark, events_jsonl):
    df = ex.load_events(spark, events_jsonl)
    assert df.count() == 600
    assert "actor" in df.columns and "payload" in df.columns


def test_flatten_events(spark, events_jsonl):
    raw = ex.load_events(spark, events_jsonl)
    out = ex.flatten_events(raw)
    assert set(out.columns) == {
        "event_id", "event_type", "created_ts", "actor_login", "repo_name", "repo_language",
    }
    assert dict(out.dtypes)["created_ts"] == "timestamp"
    assert out.filter(F.col("actor_login").isNull()).count() == 0
    assert out.count() == 600


def test_commits_per_repo(spark, events_jsonl):
    raw = ex.load_events(spark, events_jsonl)
    out = ex.commits_per_repo(raw)
    assert set(out.columns) == {"repo_name", "commits"}
    rows = out.collect()
    assert rows, "应该统计到 commit"
    counts = [r["commits"] for r in rows]
    assert counts == sorted(counts, reverse=True)
    pushes = raw.filter(F.col("type") == "PushEvent")
    expected = pushes.select(F.explode("payload.commits")).count()
    assert sum(counts) == expected


def test_top_language_by_actor(spark, events_jsonl):
    raw = ex.load_events(spark, events_jsonl)
    out = ex.top_language_by_actor(raw)
    assert set(out.columns) == {"actor_login", "repo_language", "events"}
    assert out.count() == out.select("actor_login").distinct().count(), \
        "每个 actor 只能有一行"
