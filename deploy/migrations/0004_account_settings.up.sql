-- 账号级单例设置（每用户一行）：全局隐私开关、账号自毁期限、敏感内容开关、
-- 联系人注册通知静音。此前这些 account.* RPC 为硬编码回显 stub（不持久化）：
-- account.get/setGlobalPrivacySettings、get/setAccountTTL、get/setContentSettings、
-- get/setContactSignUpNotification。本表落地真实持久化。
CREATE TABLE public.account_settings (
    user_id bigint NOT NULL,
    archive_and_mute_new_noncontact_peers boolean DEFAULT false NOT NULL,
    keep_archived_unmuted boolean DEFAULT false NOT NULL,
    keep_archived_folders boolean DEFAULT false NOT NULL,
    hide_read_marks boolean DEFAULT false NOT NULL,
    new_noncontact_peers_require_premium boolean DEFAULT false NOT NULL,
    display_gifts_button boolean DEFAULT false NOT NULL,
    noncontact_peers_paid_stars bigint DEFAULT 0 NOT NULL,
    account_ttl_days integer DEFAULT 365 NOT NULL,
    sensitive_content_enabled boolean DEFAULT false NOT NULL,
    contact_signup_silent boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT account_settings_account_ttl_days_check CHECK ((account_ttl_days > 0)),
    CONSTRAINT account_settings_noncontact_peers_paid_stars_check CHECK ((noncontact_peers_paid_stars >= 0))
);

ALTER TABLE ONLY public.account_settings
    ADD CONSTRAINT account_settings_pkey PRIMARY KEY (user_id);
