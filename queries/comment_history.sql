WITH comment_history AS (
  SELECT
    issue_id,
    array_agg(
      struct_pack(
        created_at := created_at,
        author     := comment_author,
        body       := comment_body
      ) ORDER BY created_at
    ) AS comment_history
  FROM events
  WHERE event_type = 'commented'
    AND comment_author IS NOT NULL
  GROUP BY issue_id
)

SELECT
  i.organization,
  i.repository,
  i.id           AS issue_id,
  i.number       AS issue_number,
  i.title        AS issue_title,
  COALESCE(ch.comment_history, []) AS comment_history
FROM issues i
LEFT JOIN comment_history ch
  ON i.id = ch.issue_id
ORDER BY
  i.organization,
  i.repository,
  i.id;
