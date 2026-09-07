-- bot 资料(name/about/description/commands/menu_button)变更都会 bump users.bot_info_version
-- (BumpBotInfoVersion,见 bot.sql)。群信息页的 ChannelFull.bot_info 由 RPC 层
-- channelFullBotInfoCache 缓存(键=viewer+channel,无法按 bot 定位),其跨实例失效此前只挂在
-- channel_base/channel_member 事件上——bot 自身改资料(经 BotFather 本地路径,或其它实例的
-- bots.* RPC)不会失效该缓存,导致群信息页里该 bot 的简介/命令陈旧最长 30 分钟(TTL)。
--
-- 这里给 bot 资料变更单独发一个 'bot_full' read-model 事件;ReadModelChangeListener 收到即
-- flush channelFullBotInfoCache(本地 BotFather 路径与跨实例 bots.* RPC 两条更新路径都覆盖)。
-- 与既有 user_base 事件(覆盖 RPC 投影/Redis user:base/bot 资料缓存)互补,不改动 user_base 热路径。

CREATE FUNCTION public.telesrv_notify_bot_full_read_model() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.is_bot AND (OLD.bot_info_version IS DISTINCT FROM NEW.bot_info_version) THEN
        PERFORM telesrv_bump_read_model_version('bot_full', NEW.id, 'user', NEW.id);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER users_bot_full_read_model_changed
    AFTER UPDATE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION public.telesrv_notify_bot_full_read_model();
