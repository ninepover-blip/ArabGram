-- account.updateBirthday / account.updatePersonalChannel：个人资料里的「生日」与「个人频道」。
-- 生日存月/日/年（全 0 表示未设置；year=0 表示只填月日不含年份）；个人频道存目标频道 id
-- （0 表示未设置/已清除），资料投影时按 id 取频道对象与其最新一帖 id。
ALTER TABLE public.users
  ADD COLUMN birthday_day integer DEFAULT 0 NOT NULL,
  ADD COLUMN birthday_month integer DEFAULT 0 NOT NULL,
  ADD COLUMN birthday_year integer DEFAULT 0 NOT NULL,
  ADD COLUMN personal_channel_id bigint DEFAULT 0 NOT NULL;
