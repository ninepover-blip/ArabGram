-- 登录邮箱（login email）：独立于 2FA 恢复邮箱的备用登录方式。新设备登录时验证码
-- 改投递到该邮箱（auth.sentCodeTypeEmailCode）。仅存已确认的真实地址，下发时掩码。
ALTER TABLE public.account_passwords
    ADD COLUMN login_email character varying(256) DEFAULT ''::character varying NOT NULL;
