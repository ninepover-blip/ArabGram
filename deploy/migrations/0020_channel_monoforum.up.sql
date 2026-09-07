-- 频道私信(Direct Messages)：母广播频道关联一个 monoforum 虚拟频道承载订阅者私信。
-- monoforum=true 标记该频道是 monoforum;linked_monoforum_id 双向关联母频道 ↔ monoforum。
ALTER TABLE public.channels
    ADD COLUMN monoforum boolean DEFAULT false NOT NULL;
ALTER TABLE public.channels
    ADD COLUMN linked_monoforum_id bigint DEFAULT 0 NOT NULL;
