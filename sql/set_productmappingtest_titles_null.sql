-- OPTION A: Set Title = NULL for all rows (keeps fba_updated_at unchanged)
UPDATE ProductMappingTest
SET Title = NULL;

-- OPTION B: Set Title = NULL and clear fba_updated_at (uncomment if desired)
-- UPDATE ProductMappingTest
-- SET Title = NULL,
--     fba_updated_at = NULL;
