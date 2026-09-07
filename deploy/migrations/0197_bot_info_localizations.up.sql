CREATE TABLE bot_info_localizations (
  bot_user_id bigint NOT NULL REFERENCES bots(bot_user_id) ON DELETE CASCADE,
  lang_code varchar(64) NOT NULL,
  name text,
  about text,
  description text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (bot_user_id, lang_code),
  CONSTRAINT bot_info_localizations_lang_code_nonempty CHECK (lang_code <> '')
);
