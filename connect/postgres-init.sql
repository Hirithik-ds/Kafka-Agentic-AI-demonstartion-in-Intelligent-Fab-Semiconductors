-- AIDP-FAB: PostgreSQL tables filled by the Kafka Connect JDBC sink.
SET client_min_messages = warning;
-- Runs automatically the first time the postgres container starts.
--
-- One table per Kafka topic. Each row is one Kafka event:
--   __connect_topic / __connect_partition / __connect_offset  where the event lives in Kafka (primary key,
--                                                             so replaying the topic never creates duplicates)
--   event            the full event JSON exactly as the Python services produced it (JSONB, queryable)
--   kafka_timestamp  when Kafka received the event
--
-- The v_* views below turn the JSON into normal columns for easy querying.

CREATE TABLE IF NOT EXISTS sensor_readings (
    "__connect_topic"     TEXT    NOT NULL,
    "__connect_partition" INTEGER NOT NULL,
    "__connect_offset"    BIGINT  NOT NULL,
    event                 JSONB   NOT NULL,
    kafka_timestamp       TIMESTAMP,
    PRIMARY KEY ("__connect_topic", "__connect_partition", "__connect_offset")
);
CREATE TABLE IF NOT EXISTS equipment_commands (LIKE sensor_readings INCLUDING ALL);
CREATE TABLE IF NOT EXISTS lot_events         (LIKE sensor_readings INCLUDING ALL);
CREATE TABLE IF NOT EXISTS alerts             (LIKE sensor_readings INCLUDING ALL);
CREATE TABLE IF NOT EXISTS work_orders        (LIKE sensor_readings INCLUDING ALL);
CREATE TABLE IF NOT EXISTS ai_incident_reports (LIKE sensor_readings INCLUDING ALL);

-- ---------------------------------------------------------------- views

-- Every alert from the SPC monitor and the yield monitor
CREATE OR REPLACE VIEW v_alerts AS
SELECT
    (event->>'event_time')::timestamptz      AS event_time,
    event->>'severity'                       AS severity,
    event->>'source'                         AS source,
    event->>'alert_type'                     AS alert_type,
    event->>'tool_id'                        AS tool_id,
    event->>'lot_id'                         AS lot_id,
    event->>'parameter'                      AS parameter,
    (event->>'z_score')::numeric             AS z_score,
    (event->>'yield_pct')::numeric           AS yield_pct,
    event->>'message'                        AS message
FROM alerts;

-- Maintenance work orders: one row per order, opened + completed joined together
CREATE OR REPLACE VIEW v_work_orders AS
SELECT
    o.event->>'work_order_id'                AS work_order_id,
    o.event->>'tool_id'                      AS tool_id,
    o.event->>'assigned_to'                  AS technician,
    o.event->>'priority'                     AS priority,
    o.event->>'reason'                       AS reason,
    (o.event->>'event_time')::timestamptz    AS opened_at,
    (c.event->>'event_time')::timestamptz    AS completed_at,
    c.event->>'action_taken'                 AS action_taken,
    CASE WHEN c.event IS NULL THEN 'OPEN' ELSE 'DONE' END AS status
FROM work_orders o
LEFT JOIN work_orders c
       ON c.event->>'event_type' = 'WorkOrderCompleted'
      AND c.event->>'work_order_id' = o.event->>'work_order_id'
WHERE o.event->>'event_type' = 'WorkOrderOpened';

-- Inspection result per lot (yield)
CREATE OR REPLACE VIEW v_lot_yield AS
SELECT
    event->>'lot_id'                         AS lot_id,
    (event->>'event_time')::timestamptz      AS inspected_at,
    (event->>'yield_pct')::numeric           AS yield_pct,
    (event->>'wafers_passed')::int           AS wafers_passed,
    (event->>'mean_defects')::numeric        AS mean_defects
FROM lot_events
WHERE event->>'event_type' = 'InspectionCompleted';

-- Full step-by-step history (genealogy) of every lot
CREATE OR REPLACE VIEW v_lot_history AS
SELECT
    event->>'lot_id'                         AS lot_id,
    (event->>'event_time')::timestamptz      AS event_time,
    event->>'event_type'                     AS event_type,
    event->>'step'                           AS step,
    event->>'tool_id'                        AS tool_id
FROM lot_events;

-- Sensor readings: one row per reading, parameters kept as JSON
CREATE OR REPLACE VIEW v_sensor_readings AS
SELECT
    (event->>'event_time')::timestamptz      AS event_time,
    event->>'tool_id'                        AS tool_id,
    event->>'recipe'                         AS recipe,
    (event->>'sequence')::bigint             AS sequence,
    event->'parameters'                      AS parameters
FROM sensor_readings;

-- Incident reports written by the AI root-cause assistant
CREATE OR REPLACE VIEW v_ai_incident_reports AS
SELECT
    (event->>'event_time')::timestamptz      AS created_at,
    event->>'incident_id'                    AS incident_id,
    event->>'suspect_tool'                   AS suspect_tool,
    (event->>'lots_held')::int               AS lots_held,
    (event->>'avg_yield_pct')::numeric       AS avg_yield_pct,
    event->>'work_order_id'                  AS work_order_id,
    event->>'generated_by'                   AS generated_by,
    event->>'report'                         AS report
FROM ai_incident_reports;

-- Helpful indexes for the views
CREATE INDEX IF NOT EXISTS alerts_tool_idx      ON alerts          ((event->>'tool_id'));
CREATE INDEX IF NOT EXISTS lot_events_lot_idx   ON lot_events      ((event->>'lot_id'));
CREATE INDEX IF NOT EXISTS sensor_tool_idx      ON sensor_readings ((event->>'tool_id'));
CREATE INDEX IF NOT EXISTS work_orders_wo_idx   ON work_orders     ((event->>'work_order_id'));
