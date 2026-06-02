-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Align TiDB Region boundaries with IdempotencyClaim::storage_key().
-- Token keys start with an 8-hex-character CRC32 placement prefix followed
-- by ':'. These split points produce 16 primary-key ranges by first hex digit
-- while correctness still comes from the encoded account/token payload.

ALTER TABLE idempotency_tokens ATTRIBUTES 'merge_option=deny';

SPLIT TABLE idempotency_tokens BY
    ('10000000:'),
    ('20000000:'),
    ('30000000:'),
    ('40000000:'),
    ('50000000:'),
    ('60000000:'),
    ('70000000:'),
    ('80000000:'),
    ('90000000:'),
    ('a0000000:'),
    ('b0000000:'),
    ('c0000000:'),
    ('d0000000:'),
    ('e0000000:'),
    ('f0000000:');
