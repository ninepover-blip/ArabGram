-- dialogs 按 peer_id 的反向索引。
-- dialog_light 失效扇出函数 telesrv_bump_private_dialog_light_for_user 按
-- peer_id 过滤 dialogs（"哪些用户与该 user 有私聊会话"），但 dialogs 上所有
-- 既有索引都以 user_id 前导（主键 + dialogs_user_folder_top_message_idx +
-- dialogs_user_pinned_order_idx），该按 peer_id 的查询此前只能 Seq Scan。
-- dialogs 有 CHECK (peer_type = 'user')，故单列 (peer_id) 即精确命中、无需
-- peer_type 前缀，与 contacts.contacts_contact_user_id_idx 对称。资料变更 /
-- premium 到期触发扇出时，从全表扫降为索引扫（随表规模增长收益放大）。
CREATE INDEX dialogs_peer_id_idx ON public.dialogs USING btree (peer_id);
