-- DDS ----------------------------------------------------------------------------------
-- HUBS ---------------------------------------------------------------------------------
-- Загрузка данных в таблицу STV2023081238__DWH.h_dialogs DDS
INSERT INTO STV2023081238__DWH.h_dialogs (hk_message_id, message_id, message_ts, load_dt, load_src)
select
       hash(message_id) as  hk_message_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2023081238__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV2023081238__DWH.h_dialogs) 
	AND message_id <> 0;


-- Загрузка данных в таблицу STV2023081238__DWH.h_groups DDS
INSERT INTO STV2023081238__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023081238__STAGING.groups
where hash(id) not in (select hk_group_id from STV2023081238__DWH.h_groups); 


-- Загрузка данных в таблицу STV2023081238__DWH.h_users DDS
INSERT INTO STV2023081238__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023081238__STAGING.users
where hash(id) not in (select hk_user_id from STV2023081238__DWH.h_users); 




