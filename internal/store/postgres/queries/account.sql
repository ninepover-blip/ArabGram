-- name: GetPasswordByUser :one
SELECT
  user_id, has_recovery, has_secure_values, has_password, hint,
  email_unconfirmed_pattern, login_email_pattern, secure_random
FROM account_passwords
WHERE user_id = $1;

-- name: UpsertPassword :exec
INSERT INTO account_passwords (
  user_id, has_recovery, has_secure_values, has_password, hint,
  email_unconfirmed_pattern, login_email_pattern, secure_random
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (user_id) DO UPDATE SET
  has_recovery = EXCLUDED.has_recovery,
  has_secure_values = EXCLUDED.has_secure_values,
  has_password = EXCLUDED.has_password,
  hint = EXCLUDED.hint,
  email_unconfirmed_pattern = EXCLUDED.email_unconfirmed_pattern,
  login_email_pattern = EXCLUDED.login_email_pattern,
  secure_random = EXCLUDED.secure_random,
  updated_at = now();
