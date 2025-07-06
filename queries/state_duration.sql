WITH last_close AS (
  SELECT
    issue_id,
    MAX(created_at) AS last_closed_at
  FROM events
  WHERE event_type = 'closed'
  GROUP BY issue_id
),

open_pair AS (
  SELECT
    i.organization,
    i.repository,
    i.id           AS issue_id,
    i.number       AS issue_number,
    i.title        AS issue_title,
    'open'         AS phase,
    i.created_at   AS start_at,
    lc.last_closed_at AS end_at
  FROM issues i
  JOIN last_close lc
    ON i.id = lc.issue_id
),

labeled_pair AS (
  SELECT
    i.organization,
    i.repository,
    e.issue_id,
    i.number       AS issue_number,
    i.title        AS issue_title,
    'labeled'      AS phase,
    e.created_at   AS start_at,
    COALESCE(
      (SELECT MIN(created_at)
         FROM events ue
        WHERE ue.issue_id   = e.issue_id
          AND ue.event_type = 'unlabeled'
          AND ue.created_at > e.created_at
      ),
      NOW()
    )              AS end_at
  FROM events e
  JOIN issues i
    ON i.id = e.issue_id
  WHERE e.event_type = 'labeled'
),

assigned_pair AS (
  SELECT
    i.organization,
    i.repository,
    e.issue_id,
    i.number       AS issue_number,
    i.title        AS issue_title,
    'assigned'     AS phase,
    e.created_at   AS start_at,
    COALESCE(
      (SELECT MIN(created_at)
         FROM events ue
        WHERE ue.issue_id   = e.issue_id
          AND ue.event_type = 'unassigned'
          AND ue.created_at > e.created_at
      ),
      NOW()
    )              AS end_at
  FROM events e
  JOIN issues i
    ON i.id = e.issue_id
  WHERE e.event_type = 'assigned'
),

closed_pair AS (
  SELECT
    i.organization,
    i.repository,
    lc.issue_id,
    i.number       AS issue_number,
    i.title        AS issue_title,
    'closed'       AS phase,
    lc.last_closed_at AS start_at,
    NOW()          AS end_at
  FROM last_close lc
  JOIN issues i
    ON i.id = lc.issue_id
),

combined AS (
  SELECT * FROM open_pair
  UNION ALL
  SELECT * FROM labeled_pair
  UNION ALL
  SELECT * FROM assigned_pair
  UNION ALL
  SELECT * FROM closed_pair
),

durations AS (
  SELECT
    organization,
    repository,
    issue_id,
    issue_number,
    issue_title,
    phase         AS event_type,
    start_at,
    end_at,
    date_diff('second', start_at, end_at) AS total_secs
  FROM combined
)

SELECT
  organization,
  repository,
  issue_id,
  issue_number,
  issue_title,
  event_type,
  -- format total_secs as “Xd HH:MM:SS”
  CAST(CAST(total_secs / 86400 AS BIGINT) AS VARCHAR) || 'd '
    || LPAD(CAST((total_secs % 86400) / 3600 AS VARCHAR), 2, '0') || ':'
    || LPAD(CAST((total_secs % 3600) / 60 AS VARCHAR), 2, '0') || ':'
    || LPAD(CAST(total_secs % 60 AS VARCHAR), 2, '0') AS duration
FROM durations
WHERE issue_id = '651785945'
ORDER BY
  organization,
  repository,
  issue_id,
  start_at;
