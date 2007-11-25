ALTER TABLE we_Allocation ADD events_allocated     int             default 0;
ALTER TABLE we_Allocation ADD events_missed        int             default 0;
ALTER TABLE we_Allocation ADD events_missed_cumul  int             default 0;
ALTER TABLE we_Job ADD events_allocated     int             default 0;
ALTER TABLE we_Job MODIFY status enum('register','released','create','submit','inProgress','finished','reallyFinished','failed') default 'register';

