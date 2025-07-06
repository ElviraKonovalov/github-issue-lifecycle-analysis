WITH label_events AS (
  -- grab all label add/remove events with non-null label_name
  SELECT
    i.organization,
    i.repository,
    e.issue_id,
    i.number        AS issue_number,
    i.title         AS issue_title,
    e.label_name,
    e.event_type,               -- will be 'labeled' or 'unlabeled'
    e.created_at
  FROM events e
  JOIN issues i
    ON e.issue_id = i.id
  WHERE e.event_type IN ('labeled','unlabeled')
    AND e.label_name IS NOT NULL
),

spans AS (
  -- for each 'labeled', find its matching 'unlabeled' (or NOW())
  SELECT
    le.organization,
    le.repository,
    le.issue_id,
    le.issue_number,
    le.issue_title,
    le.label_name,
    le.event_type          AS event_type,  -- this is always 'labeled' in spans
    le.created_at          AS start_at,
    COALESCE(
      (SELECT MIN(created_at)
         FROM label_events le2
        WHERE le2.issue_id   = le.issue_id
          AND le2.label_name = le.label_name
          AND le2.event_type = 'unlabeled'
          AND le2.created_at > le.created_at
      ),
      NOW()
    )                       AS end_at
  FROM label_events le
  WHERE le.event_type = 'labeled'
),

span_secs AS (
  -- compute how many seconds each span lasted
  SELECT
    organization,
    repository,
    issue_id,
    issue_number,
    issue_title,
    label_name,
    event_type,   -- still 'labeled'
    date_diff('second', start_at, end_at) AS span_secs
  FROM spans
)

-- aggregate and format
SELECT
  organization,
  repository,
  issue_id,
  issue_number,
  issue_title,
  label_name,
  event_type,     -- now visible in the final output
  CAST(CAST(SUM(span_secs) / 86400 AS BIGINT) AS VARCHAR) || 'd '
    || LPAD(CAST((SUM(span_secs) % 86400) / 3600 AS VARCHAR), 2, '0') || ':'
    || LPAD(CAST((SUM(span_secs) % 3600) / 60 AS VARCHAR), 2, '0') || ':'
    || LPAD(CAST(SUM(span_secs) % 60 AS VARCHAR), 2, '0') AS duration
FROM span_secs
GROUP BY
  organization,
  repository,
  issue_id,
  issue_number,
  issue_title,
  label_name,
  event_type
ORDER BY
  organization,
  repository,
  issue_id,
  label_name;
