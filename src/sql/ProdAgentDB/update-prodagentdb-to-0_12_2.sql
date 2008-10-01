/*
  This update only affects bosslite tables
  
  If you have not installed bosslie you do not need to run it.
*/


ALTER TABLE bl_runningjob ADD COLUMN scheduled_at_site TIMESTAMP;
ALTER TABLE bl_runningjob ADD COLUMN lfn TEXT;
ALTER TABLE bl_runningjob ADD COLUMN storage TEXT;
ALTER TABLE bl_task DROP COLUMN script_name;
ALTER TABLE bl_job DROP COLUMN log_file;
ALTER TABLE bl_job DROP COLUMN file_block;