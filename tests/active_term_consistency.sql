-- PURPOSE:
-- Validate that terms flagged as active have a positive score.
--
-- WHY THIS TEST EXISTS:
-- - is_active_term is a key behavioral metric in the mart
-- - This test ensures the flag remains consistent with its business definition

select *
from {{ ref('mart_search_trends') }}
where is_active_term = true
  and (score is null or score <= 0)