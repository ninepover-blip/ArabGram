CREATE TABLE public.seed_states (
    key text PRIMARY KEY,
    content_hash text NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL
);
