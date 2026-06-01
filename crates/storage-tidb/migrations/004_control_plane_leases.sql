-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Historical migration kept for installed catalogs. TiDB owns distributed
-- online DDL scheduling, so fresh schemas no longer add ExtendDB control-plane
-- lease columns.

UPDATE settings SET value = '0.0.4' WHERE `key` = 'catalog_version';
