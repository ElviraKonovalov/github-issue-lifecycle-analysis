WITH all_transitions AS (
  -- the “opened” event
  SELECT
    id,
    number,
    title,
    organization,
    repository,
    'opened'    AS event_type,
    created_at  AS event_at,
    user AS actor
  FROM issues

  UNION ALL

  -- all the other state changes
  SELECT
    i.id,
    i.number,
    i.title,
    i.organization,
    i.repository,
    e.event_type,
    e.created_at,
    e.actor
  FROM issues i
  JOIN events e
    ON i.id = e.issue_id
  WHERE e.event_type IN (
    'closed', 'reopened',
    'assigned', 'unassigned',
    'labeled', 'unlabeled',
    'milestoned', 'demilestoned',
    'locked', 'unlocked',
    'transferred', 'renamed'
  )
)

SELECT
  id,
  number,
  title,
  organization,
  repository,
  event_type,
  actor,
  event_at
FROM all_transitions
WHERE id = '327774900'
ORDER BY
  id,
  repository,
  organization,
  event_at;
