-- name: GetUpdateState :one
SELECT auth_key_id, user_id, pts, qts, date, seq
FROM update_states
WHERE auth_key_id = $1
  AND user_id = $2;

-- name: UpsertUpdateState :exec
INSERT INTO update_states (auth_key_id, user_id, pts, qts, date, seq)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (auth_key_id, user_id) DO UPDATE SET
  pts = GREATEST(update_states.pts, EXCLUDED.pts),
  qts = GREATEST(update_states.qts, EXCLUDED.qts),
  date = GREATEST(update_states.date, EXCLUDED.date),
  seq = GREATEST(update_states.seq, EXCLUDED.seq),
  updated_at = now();

-- name: DeleteUpdateState :exec
DELETE FROM update_states
WHERE auth_key_id = $1
  AND user_id = $2;

-- name: DeleteUpdateStatesByAuthKey :exec
DELETE FROM update_states
WHERE auth_key_id = $1;
