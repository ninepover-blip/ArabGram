-- Delay NFT item-index allocation until wallet proof is consumed, and admit
-- Core exports only while a chain-verified worker owns the collection lane.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.star_gift_ton_exports
        WHERE status IN ('resolving_item','awaiting_wallet') OR (status='failed' AND item_index<>'')
    ) THEN
        RAISE EXCEPTION 'migration 0187 requires an operator audit of pre-proof or failed exports that already reserved item indexes';
    END IF;
END;
$$;

ALTER TABLE public.star_gift_ton_exports
    DROP CONSTRAINT star_gift_ton_exports_index_uniq,
    DROP CONSTRAINT star_gift_ton_exports_shape_check;

ALTER TABLE public.star_gift_ton_exports
    ADD CONSTRAINT star_gift_ton_exports_shape_check CHECK (
        owner_user_id>0 AND octet_length(token_digest)=32 AND
        network IN ('mainnet','testnet') AND collection_address<>'' AND
        (item_index='' OR item_index ~ '^[0-9]{1,20}$') AND
        metadata_uri<>'' AND octet_length(metadata_hash)=32 AND octet_length(metadata_json) BETWEEN 2 AND 65536 AND
        status IN ('resolving_item','awaiting_wallet','proof_verified','mint_queued','prepared','submitted','confirmed','finalized','failed','quarantined') AND
        created_at>0 AND expires_at>created_at AND proof_verified_at>=0 AND submitted_at>=0 AND
        confirmed_at>=0 AND finalized_at>=0 AND version>0 AND
        (wallet_state_init_hash IS NULL OR octet_length(wallet_state_init_hash)=32) AND
        (wallet_public_key IS NULL OR octet_length(wallet_public_key)=32) AND
        (proof_hash IS NULL OR octet_length(proof_hash)=32) AND
        ((status IN ('resolving_item','awaiting_wallet') AND wallet_address='' AND proof_hash IS NULL) OR
         (status='failed' AND ((wallet_address='' AND proof_hash IS NULL) OR
                               (wallet_address<>'' AND proof_verified_at>0 AND proof_hash IS NOT NULL))) OR
         (status NOT IN ('resolving_item','awaiting_wallet','failed') AND wallet_address<>'' AND proof_verified_at>0 AND proof_hash IS NOT NULL)) AND
        ((item_index='' AND status IN ('awaiting_wallet','failed') AND expected_item_address='') OR item_index<>'') AND
        ((status IN ('resolving_item','awaiting_wallet','proof_verified') AND expected_item_address='') OR
         (status IN ('mint_queued','prepared','submitted','confirmed','finalized') AND expected_item_address<>'') OR
         status IN ('failed','quarantined')) AND
        (status NOT IN ('submitted','confirmed','finalized') OR submitted_at>0) AND
        (status NOT IN ('confirmed','finalized') OR confirmed_at>=submitted_at) AND
        (status<>'finalized' OR finalized_at>=confirmed_at)
    );

CREATE UNIQUE INDEX star_gift_ton_exports_index_uniq
    ON public.star_gift_ton_exports(network, collection_address, item_index)
    WHERE item_index<>'';

CREATE UNIQUE INDEX star_gift_ton_exports_active_lane_uniq
    ON public.star_gift_ton_exports(network, collection_address)
    WHERE item_index<>'' AND status<>'finalized';

CREATE TABLE public.star_gift_ton_admissions (
    network text NOT NULL,
    collection_address text NOT NULL,
    collection_code_hash bytea NOT NULL,
    initial_item_index numeric(20,0) NOT NULL,
    observed_next_item_index numeric(20,0) NOT NULL,
    worker_instance_id text NOT NULL,
    mint_enabled boolean NOT NULL,
    observed_at integer NOT NULL,
    lease_expires_at integer NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    PRIMARY KEY (network, collection_address),
    CONSTRAINT star_gift_ton_admissions_shape_check CHECK (
        network IN ('mainnet','testnet') AND collection_address<>'' AND
        octet_length(collection_code_hash)=32 AND
        initial_item_index>=0 AND initial_item_index<=18446744073709551615 AND
        observed_next_item_index>=0 AND observed_next_item_index<=18446744073709551615 AND
        worker_instance_id<>'' AND length(worker_instance_id)<=128 AND
        observed_at>0 AND lease_expires_at>observed_at
    )
);

CREATE INDEX star_gift_ton_admissions_lease_idx
    ON public.star_gift_ton_admissions(lease_expires_at, network, collection_address);
