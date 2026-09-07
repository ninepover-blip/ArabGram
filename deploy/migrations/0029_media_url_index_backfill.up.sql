-- Backfill URL shared-media index rows for messages whose URL semantics are
-- carried by entities or messageMediaWebPage. Future writes are handled by
-- domain.ClassifyMediaCategories; this repairs data created before that rule
-- recognized email/web_page and guards any earlier missed URL rows.

INSERT INTO channel_message_media (channel_id, id, category, message_date)
SELECT m.channel_id, m.id, 8, m.message_date
FROM channel_messages m
WHERE
    (m.media->>'kind' = 'web_page'
     OR m.entities @> '[{"type":"url"}]'::jsonb
     OR m.entities @> '[{"type":"text_url"}]'::jsonb
     OR m.entities @> '[{"type":"email"}]'::jsonb)
ON CONFLICT (channel_id, id, category) DO NOTHING;

INSERT INTO message_box_media (owner_user_id, box_id, peer_id, category, message_date)
SELECT m.owner_user_id, m.box_id, m.peer_id, 8, m.message_date
FROM message_boxes m
WHERE
    (m.media->>'kind' = 'web_page'
     OR m.entities @> '[{"type":"url"}]'::jsonb
     OR m.entities @> '[{"type":"text_url"}]'::jsonb
     OR m.entities @> '[{"type":"email"}]'::jsonb)
ON CONFLICT (owner_user_id, box_id, category) DO NOTHING;
