-- name: InsertBotUser :one
INSERT INTO users (access_hash, phone, first_name, last_name, username, country_code, is_bot, bot_info_version)
VALUES ($1, '', $2, '', $3, '', TRUE, 1)
RETURNING *;

-- name: InsertBot :exec
INSERT INTO bots (bot_user_id, owner_user_id, token_secret)
VALUES ($1, $2, $3);

-- name: GetBot :one
SELECT * FROM bots WHERE bot_user_id = $1;

-- name: ListBotsByOwner :many
-- 排除 self-owner 种子行（内置 BotFather 自有），与 memory 实现一致：/mybots 与
-- 创建上限只统计用户经 /newbot 创建的 bot。
SELECT * FROM bots WHERE owner_user_id = $1 AND bot_user_id <> owner_user_id ORDER BY bot_user_id;

-- name: CountBotsByOwner :one
SELECT COUNT(*) FROM bots WHERE owner_user_id = $1 AND bot_user_id <> owner_user_id;

-- name: UpdateBotTokenSecret :execrows
UPDATE bots SET token_secret = $2, updated_at = now() WHERE bot_user_id = $1;

-- name: UpdateBotCommandsRow :execrows
UPDATE bots SET commands = $2, updated_at = now() WHERE bot_user_id = $1;

-- name: UpdateBotDescriptionRow :execrows
UPDATE bots SET description = $2, updated_at = now() WHERE bot_user_id = $1;

-- name: UpdateBotMenuButtonRow :execrows
UPDATE bots
SET menu_button_type = $2, menu_button_text = $3, menu_button_url = $4, updated_at = now()
WHERE bot_user_id = $1;

-- name: SetBotInlinePlaceholderRow :execrows
UPDATE bots SET inline_placeholder = $2, updated_at = now() WHERE bot_user_id = $1;

-- name: SetBotInlineGeoRow :execrows
UPDATE bots SET bot_inline_geo = $2, updated_at = now() WHERE bot_user_id = $1;

-- name: SetBotNochatsRow :execrows
UPDATE bots SET bot_nochats = $2, updated_at = now() WHERE bot_user_id = $1;

-- name: SetBotChatHistoryRow :execrows
UPDATE bots SET bot_chat_history = $2, updated_at = now() WHERE bot_user_id = $1;

-- name: CanBotSendMessage :one
SELECT EXISTS (
    SELECT 1
    FROM bot_user_permissions
    WHERE bot_user_id = $1 AND user_id = $2
);

-- name: AllowBotSendMessage :one
WITH inserted AS (
    INSERT INTO bot_user_permissions (bot_user_id, user_id, from_request)
    VALUES ($1, $2, $3)
    ON CONFLICT (bot_user_id, user_id) DO NOTHING
    RETURNING 1
),
updated AS (
    UPDATE bot_user_permissions
    SET from_request = bot_user_permissions.from_request OR $3,
        updated_at = now()
    WHERE bot_user_id = $1 AND user_id = $2
      AND NOT EXISTS (SELECT 1 FROM inserted)
      AND $3
      AND NOT from_request
    RETURNING 1
)
SELECT EXISTS (SELECT 1 FROM inserted) AS created;

-- name: BumpBotInfoVersion :one
UPDATE users
SET bot_info_version = bot_info_version + 1, updated_at = now()
WHERE id = $1 AND is_bot
RETURNING bot_info_version;

-- name: UpdateBotProfileFields :one
-- 部分更新 bot 的 users 行（setBotInfo 的 name/about）；NULL 参数表示不动该字段。
UPDATE users
SET first_name = COALESCE(sqlc.narg(first_name), first_name),
    about      = COALESCE(sqlc.narg(about), about),
    updated_at = now()
WHERE id = $1 AND is_bot
RETURNING bot_info_version;

-- name: GetBotChatState :one
SELECT * FROM bot_chat_states WHERE bot_user_id = $1 AND user_id = $2;

-- name: UpsertBotChatState :exec
INSERT INTO bot_chat_states (bot_user_id, user_id, state, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (bot_user_id, user_id) DO UPDATE SET state = EXCLUDED.state, updated_at = now();

-- name: DeleteBotChatState :exec
DELETE FROM bot_chat_states WHERE bot_user_id = $1 AND user_id = $2;
