-- AIDP-FAB: continuous Flink SQL queries over the Kafka topics.
-- Submitted by "python setup_demo.py init" (runs sql-client.sh -f on this file).
-- Each INSERT starts a streaming job that never ends: it keeps reading Kafka,
-- aggregates over sliding windows and writes results to a new Kafka topic.
--
-- Windows use processing time (wall clock), so results keep coming every 10 seconds
-- even when a topic is quiet.

SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';

-- ------------------------------------------------------------------ sources

CREATE TABLE lot_events (
  event_type STRING,
  lot_id     STRING,
  step       STRING,
  tool_id    STRING,
  yield_pct  DOUBLE,
  pt AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'fab.lot.events',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-aidp-analytics',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE alerts (
  alert_type STRING,
  severity   STRING,
  tool_id    STRING,
  source     STRING,
  pt AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'fab.alerts',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-aidp-analytics',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- -------------------------------------------------------------------- sinks

CREATE TABLE line_yield (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  lots         BIGINT,
  avg_yield    DOUBLE,
  lots_failed  BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'fab.analytics.line-yield',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

CREATE TABLE tool_throughput (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  step         STRING,
  tool_id      STRING,
  lots         BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'fab.analytics.tool-throughput',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

CREATE TABLE alert_counts (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  tool_id      STRING,
  alert_type   STRING,
  alerts       BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'fab.analytics.alert-counts',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

-- -------------------------------------------------------------------- views

CREATE TEMPORARY VIEW inspections AS
  SELECT lot_id, yield_pct, pt FROM lot_events WHERE event_type = 'InspectionCompleted';

CREATE TEMPORARY VIEW completed_steps AS
  SELECT lot_id, step, tool_id, pt FROM lot_events WHERE event_type = 'StepCompleted';

CREATE TEMPORARY VIEW tool_alerts AS
  SELECT COALESCE(tool_id, 'UNKNOWN') AS tool_id, alert_type, pt FROM alerts WHERE alert_type IS NOT NULL;

-- --------------------------------------------------------------------- jobs

-- Job 1: line yield over the last minute, updated every 10 seconds
SET 'pipeline.name' = 'AIDP-FAB line yield (1 min, every 10 s)';
INSERT INTO line_yield
SELECT window_start, window_end,
       COUNT(*)                                                    AS lots,
       ROUND(AVG(yield_pct), 1)                                    AS avg_yield,
       CAST(SUM(CASE WHEN yield_pct < 90 THEN 1 ELSE 0 END) AS BIGINT) AS lots_failed
FROM TABLE(HOP(TABLE inspections, DESCRIPTOR(pt), INTERVAL '10' SECOND, INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

-- Job 2: lots finished per tool over the last minute (shows lots moving away from a tool that is down)
SET 'pipeline.name' = 'AIDP-FAB lots per tool (1 min, every 10 s)';
INSERT INTO tool_throughput
SELECT window_start, window_end, step, tool_id, COUNT(*) AS lots
FROM TABLE(HOP(TABLE completed_steps, DESCRIPTOR(pt), INTERVAL '10' SECOND, INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, step, tool_id;

-- Job 3: alerts per tool over the last minute
SET 'pipeline.name' = 'AIDP-FAB alerts per tool (1 min, every 10 s)';
INSERT INTO alert_counts
SELECT window_start, window_end, tool_id, alert_type, COUNT(*) AS alerts
FROM TABLE(HOP(TABLE tool_alerts, DESCRIPTOR(pt), INTERVAL '10' SECOND, INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, tool_id, alert_type;
