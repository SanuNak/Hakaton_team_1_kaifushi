-- Загрузка данных в таблицу  STV2023081238__STAGING.dialogs
COPY STV2023081238__STAGING.dialogs (
    message_id,
    message_ts,
    message_from,
    message_to,
    message,
    message_group
)
FROM LOCAL '/data/dialogs.csv' 
SKIP 1
DELIMITER ','
ENFORCELENGTH 
REJECTED DATA AS TABLE STV2023081238__STAGING.REJECTED_dialogs
REJECTMAX 1000	 /* 0 = ABORT ON ERROR */
ENCLOSED BY '"'
ESCAPE '\'
NULL ''; 
