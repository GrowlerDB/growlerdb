-- Probe (TASK-350 open question): does Spark's `create_changelog_view` handle a VARIANT column?
-- Run against the seeded append-only v3 table. If the CALL + SELECT succeed, changelog mode reads
-- variant; if it errors, the connector falls back to append-only snapshot-scan bootstrap for
-- variant tables. Not part of the seed — a one-off diagnostic. (Uses Spark's `to_json`, not
-- Trino's `CAST(... AS JSON)`.)
CALL stream.system.create_changelog_view(
  table => 'growlerdb.events',
  changelog_view => 'events_cl',
  compute_updates => true,
  identifier_columns => array('id'));

SELECT _change_type, id, event_type, to_json(payload) AS payload_json
FROM events_cl
ORDER BY id;
