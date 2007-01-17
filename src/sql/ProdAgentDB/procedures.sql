/** Installation and usage:
  * Install doing something like:  mysql -u root ProdAgentDB < procedures.sql
  *
  * After installation you can give the following commands:
  * call listMessages(x int,y int,short boolean);
  * call listMessagesHistory(x int,y int,short boolean);
  * call listAcquiredJobs(x int,y int,short boolean); # only meaningful if you use the prodmgr interface
  * call listAcquiredRequests(x int,y int,short boolean); # only meaningful if you use the prodmgr interface
  *
  * where x and y give a start and number of entries to retrieve (to prevent large tables)
  * short=true will return les columns (usually lacks the payload jobspec as they can contain long strings
  *
  * (assuming there are at least 5 messages) try:
  * call listMessages(0,5,true);
  *
  * the calls list 2 tables one with actual entries, another with the number of available entries
  */


DELIMITER //

DROP PROCEDURE IF EXISTS listMessages //

CREATE PROCEDURE listMessages(IN p_start INT, IN p_max INT,IN p_short BOOLEAN)
BEGIN
    SET @p_start=p_start;
    SET @p_max=p_max;
    IF p_short THEN
      PREPARE STMT FROM "SELECT ms_type.name as event,source.name as source,target.name as dest,ms_message.time,ms_message.delay FROM ms_type,ms_message, ms_process as source,ms_process as target WHERE ms_type.typeid=ms_message.type AND source.procid=ms_message.source AND target.procid=ms_message.dest ORDER BY time LIMIT ?,?";
    ELSE
      PREPARE STMT FROM "SELECT ms_type.name as event,source.name as source,target.name as dest, ms_message.payload, ms_message.time,ms_message.delay FROM ms_type,ms_message, ms_process as source,ms_process as target WHERE ms_type.typeid=ms_message.type AND source.procid=ms_message.source AND target.procid=ms_message.dest  ORDER BY time LIMIT ?,?";
    END IF;
    EXECUTE STMT USING @p_start,@p_max;
    PREPARE STMT FROM "SELECT count(*) as available_messages FROM ms_type,ms_message, ms_process as source,ms_process as target WHERE ms_type.typeid=ms_message.type AND source.procid=ms_message.source AND target.procid=ms_message.dest ";
    EXECUTE STMT ;
END;
//

DROP PROCEDURE IF EXISTS listMessageHistory //

CREATE PROCEDURE listMessageHistory(IN p_start INT, IN p_max INT,IN p_short BOOLEAN)
BEGIN
    SET @p_start=p_start;
    SET @p_max=p_max;
    IF p_short THEN
      PREPARE STMT FROM "SELECT ms_type.name as event,source.name as source,target.name as dest, ms_history.time FROM ms_type,ms_history, ms_process as source,ms_process as target WHERE ms_type.typeid=ms_history.type AND source.procid=ms_history.source AND target.procid=ms_history.dest ORDER BY time LIMIT ?,?";
    ELSE
      PREPARE STMT FROM "SELECT ms_type.name as event,source.name as source,target.name as dest, ms_history.payload,ms_history.time FROM ms_type,ms_history, ms_process as source,ms_process as target WHERE ms_type.typeid=ms_history.type AND source.procid=ms_history.source AND target.procid=ms_history.dest ORDER BY time LIMIT ?,?";
    END IF;
    EXECUTE STMT USING @p_start,@p_max;
      PREPARE STMT FROM "SELECT count(*) as available_messages FROM ms_type,ms_history, ms_process as source,ms_process as target WHERE ms_type.typeid=ms_history.type AND source.procid=ms_history.source AND target.procid=ms_history.dest ";
    EXECUTE STMT ;
END;

DROP PROCEDURE IF EXISTS listAcquiredJobs//

CREATE PROCEDURE listAcquiredJobs(IN p_start INT, IN p_max INT,IN p_short BOOLEAN)
BEGIN
    SET @p_start=p_start;
    SET @p_max=p_max;
    IF p_short THEN
      PREPARE STMT FROM "SELECT id,request_id FROM pm_job LIMIT ?,?";
    ELSE
      PREPARE STMT FROM "SELECT id,request_id,job_spec_location FROM pm_job LIMIT ?,?";
    END IF;
    EXECUTE STMT USING @p_start,@p_max;
    PREPARE STMT FROM "SELECT count(*) as available_jobs FROM pm_job";
    EXECUTE STMT ;
END;

DROP PROCEDURE IF EXISTS listAcquiredRequests//

CREATE PROCEDURE listAcquiredRequests(IN p_start INT, IN p_max INT,IN p_short BOOLEAN)
BEGIN
    SET @p_start=p_start;
    SET @p_max=p_max;
    IF p_short THEN
      PREPARE STMT FROM "SELECT id,url,priority,request_type FROM pm_request LIMIT ?,?";
    ELSE
      PREPARE STMT FROM "SELECT id,url,priority,request_type FROM pm_request LIMIT ?,?";
    END IF;
    EXECUTE STMT USING @p_start,@p_max;
    PREPARE STMT FROM "SELECT count(*) as available_requests FROM pm_request";
    EXECUTE STMT;
END;


//

DELIMITER ;


