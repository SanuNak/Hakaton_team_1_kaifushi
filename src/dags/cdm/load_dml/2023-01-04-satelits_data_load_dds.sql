-- DDS -------------------------------------------------------------------------------------
-- SATELITS --------------------------------------------------------------------------------

--s_admins
INSERT INTO STV2023081238__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt,load_src)
select la.hk_l_admin_id,
	True as is_admin,
	hg.registration_dt,
	now() as load_dt,
	's3' as load_src
from STV2023081238__DWH.l_admins as la
left join STV2023081238__DWH.h_groups as hg 
    on la.hk_group_id = hg.hk_group_id;


--s_group_name
INSERT INTO STV2023081238__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
select 
	hg.hk_group_id,
	g.group_name,
	now() as load_dt,
	's3' as load_src
from STV2023081238__DWH.h_groups  AS hg
	LEFT JOIN STV2023081238__STAGING.groups  AS g
		ON hg.group_id = g.id;


--s_group_private_status
INSERT INTO STV2023081238__DWH.s_group_private_status(hk_group_id, is_private,load_dt,load_src)
select 
	hg.hk_group_id,
	g.is_private,
	now() as load_dt,
	's3' as load_src
from STV2023081238__DWH.h_groups  AS hg
	LEFT JOIN STV2023081238__STAGING.groups  AS g
		ON hg.group_id = g.id;

--s_dialog_info
INSERT INTO STV2023081238__DWH.s_dialog_info(hk_message_id, message,message_from,message_to,load_dt,load_src)
select 
	hd.hk_message_id,
	d.message,
	d.message_from,
	d.message_to,
	now() as load_dt,
	's3' as load_src
from STV2023081238__DWH.h_dialogs  AS hd
	LEFT JOIN STV2023081238__STAGING.dialogs  AS d
		ON hd.message_id  = d.message_id;
	

--s_user_socdem
INSERT INTO STV2023081238__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
select 
	hu.hk_user_id,
	u.country,
	u.age,
	now() as load_dt,
	's3' as load_src
from STV2023081238__DWH.h_users  AS hu
	LEFT JOIN STV2023081238__STAGING.users  AS u
		ON hu.user_id  = u.id;


--s_user_chatinfo
INSERT INTO STV2023081238__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt,load_src)
select 
	hu.hk_user_id,
	u.chat_name,
	now() as load_dt,
	's3' as load_src
from STV2023081238__DWH.h_users AS hu
	LEFT JOIN STV2023081238__STAGING.users AS u
		ON hu.user_id = u.id;
        

--s_auth_history
INSERT INTO STV2023081238__DWH.s_auth_history(
    hk_l_user_group_activity, 
    user_id_from,
    "event",
    event_dt,
    load_dt,
    load_src)
select DISTINCT
    hash(hu.hk_user_id,hg.hk_group_id) as hk_l_user_group_activity, 
    gl.user_id_from, 
    gl."event",
    gl."datetime" AS event_dt,
    now() as load_dt,
    's3' load_src
from STV2023081238__STAGING.group_log as gl
left join STV2023081238__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023081238__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023081238__DWH.l_user_group_activity as luga 
    on hg.hk_group_id = luga.hk_group_id 
        and hu.hk_user_id = luga.hk_user_id;