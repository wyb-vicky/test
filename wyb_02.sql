set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;


insert overwrite table cf_tmp.wyb_table_tmp_02 partition (dt)

select
  id
  , product_id
  , request_context
  , apply_date
  , institution_id
  , dt

from

(select * from (select
  id
  , product_id
  , request_context
  , substr(create_time, 1, 10) as apply_date
  , institution_id
  , '${hivevar:date}' as dt
from rh_dwa.dwa_request_context_di
where substr(dt, 1,6) = '${hivevar:date}' and product_id = 'GPAPP-' and institution_id = 'zhichengafu'
distribute by rand() sort by rand() limit 10000
)yx_hr 
)a