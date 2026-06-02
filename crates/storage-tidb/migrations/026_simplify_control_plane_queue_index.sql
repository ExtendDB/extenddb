-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Keep TiDB catalog lifecycle work as one status-aware due-time queue.

UPDATE tables
   SET status_transition_at = CURRENT_TIMESTAMP(6)
 WHERE table_status IN ('CREATING', 'UPDATING', 'DELETING')
   AND status_transition_at IS NULL;

DROP INDEX IF EXISTS idx_tables_pending_transition ON tables;

UPDATE settings SET value = '0.0.26' WHERE `key` = 'catalog_version';
