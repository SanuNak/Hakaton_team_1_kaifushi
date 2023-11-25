-- DDS ------------------------------------------------------------------
-- HUBS -----------------------------------------------------------------

-- Удаление таблицы h_users в dds
drop table if exists STV2023081238__DWH.h_users CASCADE ;

-- Удаление таблицы h_groups в dds
drop table if exists STV2023081238__DWH.h_groups CASCADE ;

-- Удаление таблицы h_dialogs в dds
drop table if exists STV2023081238__DWH.h_dialogs CASCADE ;

-- Удаление таблицы h_group_log в dds
drop table if exists STV2023081238__DWH.h_group_log CASCADE ;


-- LINKS -----------------------------------------------------------------

-- Удаление таблицы l_user_message для dds
drop table if exists STV2023081238__DWH.l_user_message CASCADE;

-- Удаление таблицы l_admins для dds
drop table if exists STV2023081238__DWH.l_admins CASCADE;

-- Удаление таблицы l_groups_dialogs для dds
drop table if exists STV2023081238__DWH.l_groups_dialogs CASCADE;



-- SATELITS -----------------------------------------------------------------

-- Удаление таблицы s_admins для dds
drop table if exists STV2023081238__DWH.s_admins CASCADE;

-- Удаление таблицы s_group_name для dds
drop table if exists STV2023081238__DWH.s_group_name CASCADE;

-- Удаление таблицы s_group_private_status для dds
drop table if exists STV2023081238__DWH.s_group_private_status CASCADE;

-- Удаление таблицы s_dialog_info для dds
drop table if exists STV2023081238__DWH.s_dialog_info CASCADE;

-- Удаление таблицы s_user_socdem для dds
drop table if exists STV2023081238__DWH.s_user_socdem CASCADE;

-- Удаление таблицы s_user_chatinfo для dds
drop table if exists STV2023081238__DWH.s_user_chatinfo CASCADE;

-- Удаление таблицы s_auth_history для dds
drop table if exists STV2023081238__DWH.s_auth_history CASCADE;

