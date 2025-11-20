-- Migration: drop photo_path and add sub_job_id to inspection_checklist
-- Run this against your MySQL database (make a backup first).

ALTER TABLE inspection_checklist
  DROP COLUMN IF EXISTS photo_path;

ALTER TABLE inspection_checklist
  ADD COLUMN IF NOT EXISTS sub_job_id INT NULL AFTER job_id;

-- Note: If your MySQL version does not support IF NOT EXISTS for ADD COLUMN,
-- run the following instead (ensure you haven't already added the column):
-- ALTER TABLE inspection_checklist ADD COLUMN sub_job_id INT NULL AFTER job_id;

-- If you want to populate sub_job_id from existing data, you'll need a mapping
-- from 'sn' to the corresponding sub_job_id in inspection_sub_job. Example:
-- UPDATE inspection_checklist ic
-- JOIN inspection_sub_job sj ON ic.sn = sj.sn AND ic.job_id = sj.job_id
-- SET ic.sub_job_id = sj.sub_job_id;
