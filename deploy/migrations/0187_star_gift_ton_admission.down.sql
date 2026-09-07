DROP TABLE IF EXISTS public.star_gift_ton_admissions;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.star_gift_ton_exports WHERE item_index='') THEN
        RAISE EXCEPTION 'migration 0187 rollback requires all pre-proof exports to be resolved first';
    END IF;
END;
$$;

DROP INDEX IF EXISTS public.star_gift_ton_exports_active_lane_uniq;
DROP INDEX IF EXISTS public.star_gift_ton_exports_index_uniq;

ALTER TABLE public.star_gift_ton_exports
    DROP CONSTRAINT star_gift_ton_exports_shape_check;

ALTER TABLE public.star_gift_ton_exports
    ADD CONSTRAINT star_gift_ton_exports_index_uniq UNIQUE (network, collection_address, item_index),
    ADD CONSTRAINT star_gift_ton_exports_shape_check CHECK (
        owner_user_id>0 AND octet_length(token_digest)=32 AND
        network IN ('mainnet','testnet') AND collection_address<>'' AND item_index ~ '^[0-9]{1,20}$' AND
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
        ((status='resolving_item' AND expected_item_address='') OR
         (status NOT IN ('resolving_item','failed') AND expected_item_address<>'') OR
         status='failed') AND
        (status NOT IN ('submitted','confirmed','finalized','quarantined') OR submitted_at>0) AND
        (status NOT IN ('confirmed','finalized') OR confirmed_at>=submitted_at) AND
        (status<>'finalized' OR finalized_at>=confirmed_at)
    );
