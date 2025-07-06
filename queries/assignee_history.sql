-- Assignee History Query
WITH assignee_history AS (
  SELECT
    issue_id,
    array_agg(
      struct_pack(
        event_type := event_type,
        at         := created_at,
        assignee   := assignee_name
      ) ORDER BY created_at
    ) AS assignee_history
  FROM events
  WHERE event_type IN ('assigned','unassigned')
    AND assignee_name IS NOT NULL
  GROUP BY issue_id
)

SELECT
  i.organization,
  i.repository,
  i.id           AS issue_id,
  i.number       AS issue_number,
  i.title        AS issue_title,
  COALESCE(ah.assignee_history, []) AS assignee_history
FROM issues i
LEFT JOIN assignee_history ah
  ON i.id = ah.issue_id
ORDER BY
  i.organization,
  i.repository,
  i.id;
