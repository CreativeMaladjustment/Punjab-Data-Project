-- Drop llm_extractions.raw_response.
--
-- It was a JSON copy of each extraction's parsed entries, which catalogue_entries holds
-- row by row. The extraction scripts no longer write it and scripts/extraction_status.py
-- now counts catalogue_entries rows instead. (The model's raw reply is kept separately in
-- llm_extractions.raw_text.)
--
-- DROP COLUMN only marks the column dropped; run the "Vacuum full" workflow afterwards to
-- rewrite llm_extractions and return the space.

alter table llm_extractions drop column if exists raw_response;
