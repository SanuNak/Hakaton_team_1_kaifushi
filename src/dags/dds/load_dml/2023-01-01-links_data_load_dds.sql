
-- DDS -----------------------------------------------------------------------------------
-- LINKS ---------------------------------------------------------------------------------

-- Загрузка данных в таблицу STV2023081238__DWH.l_admins DDS
INSERT INTO STV2023081238__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
	hash(hg.hk_group_id,hu.hk_user_id),
	hg.hk_group_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
from STV2023081238__STAGING.groups as g
	left join STV2023081238__DWH.h_users as hu on g.admin_id = hu.user_id
	left join STV2023081238__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV2023081238__DWH.l_admins);


-- Загрузка данных в таблицу STV2023081238__DWH.l_user_message DDS
INSERT INTO STV2023081238__DWH.l_user_message(hk_l_user_message, hk_user_id , hk_message_id,load_dt,load_src)
select
	hash(hu.hk_user_id, hd.hk_message_id),
	hu.hk_user_id,
	hd.hk_message_id,
	now() as load_dt,
	's3' as load_src
from STV2023081238__STAGING.dialogs as d
	left join STV2023081238__DWH.h_users as hu on d.message_from = hu.user_id
	left join STV2023081238__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from STV2023081238__DWH.l_user_message)
	AND hu.hk_user_id IS NOT NULL
	AND hd.hk_message_id IS NOT NULL;


-- Загрузка данных в таблицу STV2023081238__DWH.l_groups_dialogs DDS
INSERT INTO STV2023081238__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id , hk_group_id, load_dt, load_src)
select
	hash(hd.hk_message_id, hg.hk_group_id),
	hd.hk_message_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2023081238__STAGING.dialogs as d
	left join STV2023081238__DWH.h_dialogs as hd on d.message_id = hd.message_id
	left join STV2023081238__DWH.h_groups as hg on d.message_group = hg.group_id
where hash(hd.hk_message_id, hg.hk_group_id) not in (select hk_l_groups_dialogs from STV2023081238__DWH.l_groups_dialogs)
	AND hg.hk_group_id IS NOT NULL;


-- Загрузка данных в таблицу STV2023081238__DWH.l_user_group_activity DDS
INSERT INTO STV2023081238__DWH.l_user_group_activity(
       hk_l_user_group_activity, 
       hk_user_id,
       hk_group_id,
       load_dt,
       load_src
       )
SELECT DISTINCT
	hash(hu.hk_user_id,hg.hk_group_id),
       hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
FROM STV2023081238__STAGING.group_log AS gl
LEFT JOIN STV2023081238__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN STV2023081238__DWH.h_groups AS hg ON gl.group_id = hg.group_id; 