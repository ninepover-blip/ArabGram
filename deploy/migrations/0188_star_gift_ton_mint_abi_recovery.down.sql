DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.star_gift_ton_recoveries) THEN
        RAISE EXCEPTION 'migration 0188 rollback refuses to erase TON export recovery audit evidence';
    END IF;
END;
$$;

DROP TABLE public.star_gift_ton_recoveries;

ALTER TABLE public.star_gift_ton_admissions
    DROP CONSTRAINT star_gift_ton_admissions_mint_abi_check,
    DROP COLUMN mint_abi;

ALTER TABLE public.star_gift_ton_exports
    DROP CONSTRAINT star_gift_ton_exports_mint_abi_check,
    DROP COLUMN mint_abi;
