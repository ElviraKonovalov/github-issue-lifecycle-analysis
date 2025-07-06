WITH last_close AS (
  SELECT
    issue_id,
    MAX(created_at) AS last_closed_at
  FROM events
  WHERE event_type = 'closed'
  GROUP BY issue_id
)

SELECT
  i.organization,
  i.repository,
  i.id             AS issue_id,
  i.number         AS issue_number,
  i.title          AS issue_title,
  i.created_at     AS opened_at,
  lc.last_closed_at AS closed_at,
  -- compute raw seconds between open and final close
  date_diff('second', i.created_at, lc.last_closed_at)                  AS resolution_secs,
  -- format as “Xd HH:MM:SS”
  CAST(CAST(resolution_secs / 86400   AS BIGINT) AS VARCHAR) || 'd '
    || LPAD(CAST((resolution_secs % 86400) / 3600   AS VARCHAR), 2, '0') || ':'
    || LPAD(CAST((resolution_secs % 3600)  / 60     AS VARCHAR), 2, '0') || ':'
    || LPAD(CAST(resolution_secs        % 60       AS VARCHAR), 2, '0')
    AS resolution_duration
FROM issues i
JOIN last_close lc
  ON i.id = lc.issue_id
ORDER BY
  opened_at;
