SELECT
  i.id           AS issue_id,
  i.number       AS issue_number,
  i.repository,
  i.organization,
  COUNT(*)       AS reopened_count
FROM issues i
JOIN events e
  ON i.id = e.issue_id
WHERE e.event_type = 'reopened' 
GROUP BY
  i.id,
  i.number,
  i.repository,
  i.organization
ORDER BY
  reopened_count DESC;
