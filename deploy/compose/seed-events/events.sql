-- Seed an append-only Iceberg v3 table with a VARIANT column (D47/D48, TASK-348).
--
-- `growlerdb.events` is GitHub-events-shaped: scalar key/time columns + an event-type
-- discriminator + a semi-structured `payload` VARIANT whose per-row structure differs by event
-- type. format-version=3 is required for VARIANT; the table stays append-only (no deletes/DVs) so
-- the connector's changelog read is well-defined and iceberg-rust could read the data files once
-- it parses v3 variant schemas. Run via `spark-sql -f` against the REST catalog (`just variant`).
--
-- Spark 4.1 `parse_json(...)` produces a VARIANT value; Iceberg 1.11 writes it (shredded where the
-- writer chooses). The demo index (`deploy/compose/events.yaml`) maps `payload` as VARIANT with a
-- flatten catch-all plus two shapes selected by the sibling `event_type` discriminator.

CREATE TABLE IF NOT EXISTS stream.growlerdb.events (
  id          STRING,
  ts          BIGINT,   -- event time, epoch millis
  event_type  STRING,   -- discriminator (a sibling column)
  payload     VARIANT   -- semi-structured, per-row shape
) USING iceberg
TBLPROPERTIES ('format-version' = '3');

INSERT INTO stream.growlerdb.events VALUES
  ('evt-1', 1782000000000, 'PullRequestEvent',
   parse_json('{"action":"opened","number":1347,"title":"Add Iceberg v3 variant support","merged":false,"user":{"login":"octocat","id":583231}}')),
  ('evt-2', 1782000060000, 'PullRequestEvent',
   parse_json('{"action":"closed","number":1290,"title":"Refactor the flatten path","merged":true,"user":{"login":"hubot","id":9919}}')),
  ('evt-3', 1782000120000, 'IssuesEvent',
   parse_json('{"action":"opened","number":88,"title":"Flaky test on CI","labels":["bug","ci"],"user":{"login":"defunkt","id":2}}')),
  ('evt-4', 1782000180000, 'IssuesEvent',
   parse_json('{"action":"closed","number":42,"title":"Docs typo in the query guide","labels":["docs"],"user":{"login":"mojombo","id":1}}')),
  ('evt-5', 1782000240000, 'WatchEvent',
   parse_json('{"action":"started","user":{"login":"octocat","id":583231}}')),
  ('evt-6', 1782000300000, 'PullRequestEvent',
   parse_json('{"action":"opened","number":1351,"title":"Wire the Trino hydration seam","merged":false,"user":{"login":"octocat","id":583231}}'));
