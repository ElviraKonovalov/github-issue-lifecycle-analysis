SELECT
  e.actor                                  AS contributor,
  COUNT(DISTINCT i.organization || '/' || i.repository) AS repos_participated,
  COUNT(*)                                  AS total_events,
  array_agg(DISTINCT i.organization || '/' || i.repository) AS repositories
FROM events e
JOIN issues i
  ON e.issue_id = i.id
GROUP BY
  e.actor
ORDER BY
  repos_participated DESC,
  total_events DESC;
