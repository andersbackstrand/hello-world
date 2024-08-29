CREATE OR REPLACE PACKAGE BODY DMSPST.BET_LIMIT_JOB as

  gc_job_name constant varchar2(30) := 'bet_limit_scheduler_job';
  gc_package constant varchar2(30) := 'bet_limit_job';

  --Message_type som kommer från Solace/java-adaptern
  --betlimitvr constant varchar2(512) := 'atg-service-responsible-gambling/restriction/virtual-racing/limit-created';
  betlimitnetlosscasinoweek     constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/netlosslimit/CASINO/week/limit-created';
  betlimitnetlosshorseweek      constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/netlosslimit/HORSE_BETTING/week/limit-created';
  betlimitnetlosssportweek      constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/netlosslimit/SPORTSBOOK/week/limit-created';
  betlimitplaytimecasinoday     constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/playtimelimit/CASINO/day/limit-created';
  betlimitplaytimecasinoweek    constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/playtimelimit/CASINO/week/limit-created';
  betlimitplaytimecasinomonth   constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/playtimelimit/CASINO/month/limit-created';
  betlimitselfexclusioncasino   constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/selfexclusion/CASINO';
  betlimitselfexclusionsport    constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/selfexclusion/SPORTSBOOK';
  betlimitselfexclusionhorse    constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/selfexclusion/HORSE_BETTING';
  betlimitdepositcreated        constant varchar2(512) := 'atg-service-payment/deposit-limit';
  betlimitlogintimeday          constant varchar2(512) := 'atg-service-responsible-gambling/restriction/login/day/limit-changed';
  betlimitlogintimeweek         constant varchar2(512) := 'atg-service-responsible-gambling/restriction/login/week/limit-changed';
  betlimitlogintimemonth        constant varchar2(512) := 'atg-service-responsible-gambling/restriction/login/month/limit-changed';
  --betlimitdepositdeleted constant varchar2(512) := 'atg-service-payment/deposit-limit/deleted';
  --betlimitdepositcustomeractivated constant varchar2(512) := 'atg-service-payment/deposit-limit/customer-activated'; 
  
  --Strängar för att kolla felloggs-tabellerna samt för hantering av filskick och loggning
  --betlimitstake constant varchar2(512) := 'BET_LIMIT_STAKE';
  --betlimitbettime constant varchar2(512) := 'BET_LIMIT_BET_TIME';
  betlimitnetloss constant varchar2(512) := 'BET_LIMIT_NET_LOSS';
  betlimitplaytime constant varchar2(512) := 'BET_LIMIT_PLAY_TIME';
  betlimitselfexclusion constant varchar2(512) := 'BET_LIMIT_SELF_EXCLUSION';
  betlimitdeposit constant varchar2(512) := 'BET_LIMIT_DEPOSIT';
  betlimitlogintime constant varchar2(512) := 'BET_LIMIT_LOGIN_TIME';
  

  
  -- Event i denna tabell kan styra andra event. 
  -- Är det max limit för en deposit limit så finns en rad i denna tabell för max limit
  -- och samtidigt triggas en betlimitdeposit för denna kund om max limit är lägre. 
  betlimitdepositmaxlimit constant varchar2(512) := 'atg-service-payment/maxlimit';
  
  betlimitsetmaxlimit constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/maxlimit/SET';
  betlimitdeletemaxlimit constant varchar2(512) := 'atg-service-responsible-gambling/event/restriction/maxlimit/DELETE';  


  date_time_format        constant varchar2(512) := 'yyyy-mm-dd hh24:mi:ss';
  source_time_zone        constant varchar2(512) := 'UTC';

function to_local_date_time(p_date_time varchar2, p_time_zone varchar2, p_date_format varchar2) return date result_cache is
  pragma udf;
  lc_prc varchar2(30) := 'to_local_date_time';
begin
   return to_date(to_char(cast(to_timestamp_tz(p_date_time||' '||p_time_zone,p_date_format||' TZR') as timestamp with local time zone), p_date_format),p_date_format);
exception
   when others then
    return p_date_time;
    raise;
end to_local_date_time;


function get_punter_id (p_account_index number) return number result_cache is

pragma udf;

  lc_prc varchar2(30) := 'get_punter_id';
  lv_ret spst.punter_current.punter_id%type := null;
  lv_punter_id spst.punter_current.punter_id%type;

begin

    select punter_id into lv_punter_id
      from spst.punter_current
     where acc_index = p_account_index
       and kontotyp = 'PERSON';

    lv_ret := lv_punter_id;

  return lv_ret;

exception
  when others then
    return lv_ret;

end get_punter_id;

function get_punter_key (p_account_index number) return number result_cache is

pragma udf;

  lc_prc varchar2(30) := 'get_punter_id';
  lv_ret spst.punter_current.punter_key%type;
  lv_punter_key spst.punter_current.punter_key%type;

begin

    select punter_key into lv_punter_key
      from spst.punter_current
     where acc_index = p_account_index
       and kontotyp = 'PERSON';

    lv_ret := lv_punter_key;

  return lv_ret;

exception
  when others then
    return lv_ret;

end get_punter_key;

/*procedure load_bet_limit_bet_time_changes(p_message clob, p_message_type varchar2)
is

  lc_prc varchar2(50) := 'load_bet_limit_bet_time_changes';

begin

  insert into spst.bet_limit_bet_time_changes (acc_index, 
                                               punter_key, 
                                               punter_id, 
                                               bet_limit_type_id,
                                               amount,
                                               bet_vertical_id,
                                               valid_from,
                                               valid_to,
                                               lock_date,
                                               created_date,
                                               updated_date,
                                               spst_created_date )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              case p_message_type
                when betlimitvr then
                  3
                else null 
              end bet_limit_type_id,
              amount,
              case p_message_type
                when betlimitvr then
                  2
                else null 
              end bet_vertical_id,
              substr((replace(valid_from,'T',' ')),1,19), --to_local_date_time(substr((replace(valid_from,'T',' ')),1,19),source_time_zone,date_time_format),
              null,
              substr((replace(lock_date,'T',' ')),1,19),--to_local_date_time(substr((replace(lock_date,'T',' ')),1,19),source_time_zone,date_time_format),
              substr((replace(created_date,'T',' ')),1,19),--to_local_date_time(substr((replace(created_date,'T',' ')),1,19),source_time_zone,date_time_format),
              substr((replace(updated_date,'T',' ')),1,19),--to_local_date_time(substr((replace(updated_date,'T',' ')),1,19),source_time_zone,date_time_format),
              sysdate
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   punter_key varchar2(255) path '$.punterId',
                                   amount varchar2(255) path '$.sessionTimeLimit',
                                   created_date varchar2(255) path '$.createdOn',
                                   updated_date varchar2(255) path '$.updatedOn',
                                   valid_from varchar2(255) path '$.validFrom',
                                   lock_date varchar2(255) path '$.lockDate')) jt
         log errors into spst.err$_bet_limit_bet_time_changes('spst.bet_limit_bet_time_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_bet_limit_bet_time_changes;*/

/*procedure load_bet_limit_stake_changes(p_message clob)
is

  lc_prc varchar2(50) := 'load_bet_limit_bet_time_changes';

begin

  insert into spst.bet_limit_stake_changes (acc_index, 
                                            punter_key, 
                                            punter_id, 
                                            bet_limit_type_id,
                                            amount,
                                            bet_vertical_id,
                                            valid_from,
                                            valid_to,
                                            lock_date,
                                            created_date,
                                            updated_date,
                                            spst_created_date )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              4,
              amount/100,
              2,
              substr((replace(valid_from,'T',' ')),1,19),--to_local_date_time(substr((replace(valid_from,'T',' ')),1,19),source_time_zone,date_time_format),
              null,
              substr((replace(lock_date,'T',' ')),1,19),--to_local_date_time(substr((replace(lock_date,'T',' ')),1,19),source_time_zone,date_time_format),
              substr((replace(created_date,'T',' ')),1,19),--to_local_date_time(substr((replace(created_date,'T',' ')),1,19),source_time_zone,date_time_format),
              substr((replace(updated_date,'T',' ')),1,19),--to_local_date_time(substr((replace(updated_date,'T',' ')),1,19),source_time_zone,date_time_format),
              sysdate
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   punter_key varchar2(255) path '$.punterId',
                                   amount varchar2(255) path '$.stakeLimit',
                                   created_date varchar2(255) path '$.createdOn',
                                   updated_date varchar2(255) path '$.updatedOn',
                                   valid_from varchar2(255) path '$.validFrom',
                                   lock_date varchar2(255) path '$.lockDate')) jt
         log errors into spst.err$_bet_limit_stake_changes('spst.bet_limit_stake_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_bet_limit_stake_changes;*/

procedure load_bet_limit_play_time_changes(p_message clob, p_message_type in varchar2)
is

  lc_prc varchar2(50) := 'load_bet_limit_play_time_changes';

begin

  insert into spst.bet_limit_play_time_changes (acc_index, 
                                            punter_key, 
                                            punter_id, 
                                            bet_limit_type_id,
                                            limit_value,
                                            bet_vertical_id,
                                            valid_from,
                                            valid_to,
                                            updated_date,
                                            spst_created_date )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              case p_message_type
                when betlimitplaytimecasinoday then 13
                when betlimitplaytimecasinoweek then 14
                when betlimitplaytimecasinomonth then 15
              end bet_limit_type_id,
              case p_message_type
                when betlimitplaytimecasinoday then limit_value_day
                when betlimitplaytimecasinoweek then limit_value_week
                when betlimitplaytimecasinomonth then limit_value_month
              end limit_value,
              3,
              case p_message_type
                when betlimitplaytimecasinoday then CAST (FROM_TZ (valid_from_day, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date)  --substr((replace(valid_from_day,'T',' ')),1,19)
                when betlimitplaytimecasinoweek then CAST (FROM_TZ (valid_from_week, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_from_week,'T',' ')),1,19)
                when betlimitplaytimecasinomonth then CAST (FROM_TZ (valid_from_month, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_from_month,'T',' ')),1,19)
              end valid_from,
              null, --valid_to
              CAST (FROM_TZ (updated_date, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) updated_date, --substr((replace(updated_date,'T',' ')),1,19),
              sysdate
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   updated_date timestamp(9) path '$.timestamp',
                                   nested path '$.playTimeLimit.CASINO.day'
                                     columns ( limit_value_day varchar2(255) path '$.limit.value',
                                               valid_from_day timestamp(9) path '$.validFrom'
                                              ),
                                   nested path '$.playTimeLimit.CASINO.week'
                                     columns ( limit_value_week varchar2(255) path '$.limit.value',
                                               valid_from_week timestamp(9) path '$.validFrom'
                                              ),
                                  nested path '$.playTimeLimit.CASINO.month'
                                     columns ( limit_value_month varchar2(255) path '$.limit.value',
                                               valid_from_month timestamp(9) path '$.validFrom'
                                              )
                                   )
                         ) jt
         log errors into spst.err$_bet_limit_play_time_changes('spst.bet_limit_play_time_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_bet_limit_play_time_changes;

procedure load_bet_limit_deposit_changes(p_message clob, p_message_type in varchar2)
is

  lc_prc varchar2(50) := 'load_bet_limit_deposit_changes';

begin

  insert into spst.bet_limit_deposit_changes (acc_index, 
                                            punter_key, 
                                            punter_id, 
                                            bet_limit_type_id,
                                            limit_value,
                                            bet_vertical_id,
                                            valid_from,
                                            valid_to,
                                            updated_on,
                                            updated_by,
                                            message_timestamp,
                                            message_type,
                                            source_system,
                                            spst_created_date )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              case limit_period
                when 'day' then 5
                when 'week' then 6
                when 'month' then 7
              end bet_limit_type_id,
              limit_value/100 as limit_value,
              0, --alltid alla vertikaler
              CAST (FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) as valid_from,
              CAST (FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) as valid_to,
              CAST (FROM_TZ (updated_on, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) as updated_on,
              updated_by,
              CAST (FROM_TZ (message_timestamp, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) as message_timestamp,
              --to_local_date_time(substr((replace(valid_from,'T',' ')),1,19),source_time_zone,date_time_format) as valid_from,
              --to_local_date_time(substr((replace(valid_to,'T',' ')),1,19),source_time_zone,date_time_format) as valid_to,
              --to_local_date_time(substr((replace(updated_date,'T',' ')),1,19),source_time_zone,date_time_format) as updated_date, --substr((replace(updated_date,'T',' ')),1,19) as updated_date,
              message_type,
              'PAYMENT' as source_system,
              sysdate
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   message_timestamp timestamp(9) path '$.timestamp',
                                   limit_value varchar2(255) path '$.value',
                                   limit_period varchar2(255) path '$.period',
                                   valid_from timestamp(9) path '$.validFrom',
                                   valid_to timestamp(9) path '$.validTo',
                                   updated_on timestamp(9) path '$.updatedOn',
                                   updated_by varchar2(255) path '$.updatedBy',
                                   message_type varchar2(255) path '$.type'
                                   )
                         ) jt
         log errors into spst.err$_bet_limit_deposit_changes('spst.bet_limit_deposit_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_bet_limit_deposit_changes;

procedure load_bet_limit_net_loss_changes(p_message clob, p_message_type in varchar2)
is

  lc_prc varchar2(50) := 'load_bet_limit_net_loss_changes';

begin

  insert into spst.bet_limit_net_loss_changes (acc_index, 
                                            punter_key, 
                                            punter_id, 
                                            bet_limit_type_id,
                                            limit_value,
                                            bet_vertical_id,
                                            valid_from,
                                            valid_to,
                                            updated_date,
                                            spst_created_date )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              8,
              --limit_value/100,
              case p_message_type
                when betlimitnetlosscasinoweek then limit_value_casino/100
                when betlimitnetlosshorseweek then limit_value_horse/100
                when betlimitnetlosssportweek then limit_value_sport/100
              end limit_value,
              case p_message_type
                when betlimitnetlosscasinoweek then 3
                when betlimitnetlosshorseweek then 1
                when betlimitnetlosssportweek then 4
              end bet_vertical_id,
              --3,
              case p_message_type
                when betlimitnetlosscasinoweek then CAST (FROM_TZ (valid_from_casino, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_from_casino,'T',' ')),1,19)
                when betlimitnetlosshorseweek then  CAST (FROM_TZ (valid_from_horse, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_from_horse,'T',' ')),1,19)
                when betlimitnetlosssportweek then  CAST (FROM_TZ (valid_from_sport, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_from_sport,'T',' ')),1,19)
              end valid_from,
              --substr((replace(valid_from,'T',' ')),1,19),--to_local_date_time(substr((replace(valid_from,'T',' ')),1,19),source_time_zone,date_time_format),
              case p_message_type
                when betlimitnetlosscasinoweek then CAST (FROM_TZ (valid_to_casino, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_to_casino,'T',' ')),1,19)
                when betlimitnetlosshorseweek then  CAST (FROM_TZ (valid_to_horse, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_to_horse,'T',' ')),1,19)
                when betlimitnetlosssportweek then  CAST (FROM_TZ (valid_to_sport, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) --substr((replace(valid_to_sport,'T',' ')),1,19)
              end valid_to, --valid_to
              CAST (FROM_TZ (updated_date, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) as updated_date, --substr((replace(updated_date,'T',' ')),1,19),
              sysdate
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   updated_date timestamp(9) path '$.timestamp',
                                   nested path '$.netLossLimit.CASINO.week'
                                     columns ( limit_value_casino varchar2(255) path '$.limit.value',
                                               valid_from_casino timestamp(9) path '$.validFrom',
                                               valid_to_casino timestamp(9) path '$.validTo'
                                              ),
                                   nested path '$.netLossLimit.HORSE_BETTING.week'
                                     columns ( limit_value_horse varchar2(255) path '$.limit.value',
                                               valid_from_horse timestamp(9) path '$.validFrom',
                                               valid_to_horse timestamp(9) path '$.validTo'
                                              ),
                                   nested path '$.netLossLimit.SPORTSBOOK.week'
                                     columns ( limit_value_sport varchar2(255) path '$.limit.value',
                                               valid_from_sport timestamp(9) path '$.validFrom',
                                               valid_to_sport timestamp(9) path '$.validTo'
                                              )
                                   )
                         ) jt
         log errors into spst.err$_bet_limit_net_loss_changes('spst.bet_limit_net_loss_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_bet_limit_net_loss_changes;

procedure load_bet_limit_self_exclusion_changes(p_message clob, p_message_type in varchar2)
is

  lc_prc varchar2(50) := 'load_bet_limit_self_exclusion_changes';

begin

  insert into spst.bet_limit_self_exclusion_changes ( acc_index, 
                                                      punter_key, 
                                                      punter_id,
                                                      bet_limit_type_id,
                                                      limit_duration,
                                                      limit_kind,
                                                      temporary_exclusion,
                                                      bet_vertical_id,
                                                      valid_from,
                                                      valid_to,
                                                      excluded_until,
                                                      locked_until,
                                                      updated_date,
                                                      spst_created_date )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              9 bet_limit_type_id,
              limit_duration,
              limit_kind,
              temporary_exclusion,
              case p_message_type
                when betlimitselfexclusioncasino then 3
                when betlimitselfexclusionsport then 4
                when betlimitselfexclusionhorse then 1
              end bet_vertical_id,
              CAST (FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) as valid_from, --substr((replace(valid_from,'T',' ')),1,19) valid_from,
              CAST (FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) as valid_to, --substr((replace(valid_to,'T',' ')),1,19) valid_to,
              CAST (FROM_TZ (excluded_until, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) as excluded_until, --substr((replace(excluded_until,'T',' ')),1,19) excluded_until,
              CAST (FROM_TZ (locked_until, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) as locked_until, --substr((replace(locked_until,'T',' ')),1,19) locked_until,
              CAST (FROM_TZ (updated_date, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) as updated_date, --substr((replace(updated_date,'T',' ')),1,19) updated_date,
              sysdate
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   updated_date timestamp(9) path '$.timestamp."$date"',
                                   limit_kind varchar2(255) path '$.kind',
                                   limit_duration varchar2(255) path '$.duration',
                                   excluded_until timestamp(9) path '$.excludedUntil',
                                   valid_to timestamp(9) path '$.validTo',
                                   temporary_exclusion varchar2(255) path '$.temp_exclusion',
                                   valid_from timestamp(9) path '$.validFrom',
                                   locked_until timestamp(9) path '$.lockedUntil'
                                                                      )
                         ) jt
         log errors into spst.err$_bet_limit_self_exclusion_changes('spst.bet_limit_self_exclusion_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_bet_limit_self_exclusion_changes;

procedure load_bet_limit_login_time_changes (p_message clob) is
lc_prc varchar2(50) := 'load_bet_limit_login_time_changes';
begin
  insert into spst.bet_limit_login_time_changes (msg_id
                                                ,limit_id
                                                ,acc_index
                                                ,punter_key
                                                ,punter_id
                                                ,bet_limit_type_id
                                                ,limit_value
                                                ,bet_vertical_id
                                                ,valid_from
                                                ,valid_to
                                                ,msg_timestamp_utc
                                                ,msg_created_by
                                                ,msg_type
                                                ,created_on)
       select msg_id,
              limit_id,
              acc_index,
              get_punter_key(jt.acc_index) punter_key,
              get_punter_id(jt.acc_index) punter_id,
              case limit_period
                when 'day' then 10
                when 'week' then 11
                when 'month' then 12
              end bet_limit_type_id,
              limit_value limit_value,  -- minutes
              0 bet_vertical_id,
              CAST (FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) valid_from,
              CAST (FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS date) valid_to,
              msg_timestamp_utc,
              coalesce(msg_created_by, 'Unknown') msg_created_by,
              msg_type,
              sysdate created_on
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (msg_id varchar2(255) path '$.id',
                                   limit_id number path '$.limitId',
                                   acc_index varchar2(255) path '$.accountIndex',
                                   msg_timestamp_utc timestamp(9) path '$.timestamp',
                                   limit_value number path '$.value',
                                   limit_period varchar2(255) path '$.period',
                                   valid_from timestamp(9) path '$.validFrom',
                                   valid_to timestamp(9) path '$.validTo',
                                   msg_created_by varchar2(255) path '$.createdBy',
                                   msg_type varchar2(255) path '$.type'
                                  )
                         ) jt
         log errors into spst.err$_bet_limit_login_time_changes('spst.bet_limit_login_time_changes') reject limit unlimited;
exception
   when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end load_bet_limit_login_time_changes;

procedure load_bet_limit_deposit_max_limit_changes (p_message clob, p_event_type varchar2) is
lc_prc varchar2(50) := 'load_bet_limit_deposit_max_limit_changes';
begin


    insert into spst.bet_limit_max_changes (
       acc_index, punter_key, punter_id, 
       bet_limit_type_id, limit_value, unit, 
       bet_vertical_id, valid_from_utc, valid_to_utc, 
       valid_from_se, valid_to_se, event_type, 
       message_timestamp_utc, message_type, created_on, 
       created_by, updated_on, updated_by, message_id)       
           select acc_index,
                  get_punter_key(jt.acc_index) punter_key,
                  get_punter_id(jt.acc_index) punter_id,
                  case limit_period
                    when 'day' then 5
                    when 'week' then 6
                    when 'month' then 7
                  end bet_limit_type_id,
                  amount as limit_value,
                  'CENTS' as unit,  -- Hard coded because it's not in the event
                  0 bet_vertical_id, -- All verticals = 0
                  FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'UTC' valid_from_utc,
                  FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'UTC' valid_to_utc,
                  FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'Europe/Stockholm' valid_from_se,
                  FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'Europe/Stockholm' valid_to_se,
                  p_event_type as event_type,
                  msg_timestamp_utc as message_timestamp_utc,              
                  message_type as message_type,
                  sysdate created_on,
                  coalesce(msg_created_by, 'Unknown') created_by,
                  null as updated_on,
                  null as updated_by,
                  msg_id as message_id
             from json_table (
                              p_message,
                              '$'
                              columns (msg_id               varchar2(255) path '$.id',
                                       message_type         varchar2(255) path '$.type',
                                       acc_index            varchar2(255) path '$.maxLimit.accountIndex',
                                       msg_timestamp_utc    timestamp(9) path '$.timestamp',
                                       amount               number path '$.maxLimit.amount',
                                       limit_period         varchar2(255) path '$.maxLimit.period',
                                       valid_from           timestamp(9) path '$.maxLimit.validFrom',
                                       valid_to             timestamp(9) path '$.maxLimit.validTo',
                                       msg_created_by       varchar2(255) path '$.createdBy'
                                      )
                             ) jt
               log errors into spst.err$_bet_limit_max_changes('load_bet_limit_deposit_max_limit_changes') reject limit unlimited;

exception
   when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end load_bet_limit_deposit_max_limit_changes;


procedure load_bet_limit_max_limit_changes (p_message clob, p_event_type varchar2) is
lc_prc varchar2(50) := 'load_bet_limit_max_limit_changes';
lv_event_type varchar2(50);
begin

        if p_event_type=betlimitsetmaxlimit then
           lv_event_type := 'SET';
        elsif p_event_type=betlimitdeletemaxlimit then
           lv_event_type := 'DELETE';
        end if;


        insert into spst.bet_limit_max_changes (
                    acc_index, punter_key, punter_id, 
                    bet_limit_type_id, limit_value, unit, 
                    bet_vertical_id, valid_from_utc, valid_to_utc, 
                    valid_from_se, valid_to_se, event_type, 
                    message_timestamp_utc, message_type, created_on, 
                    created_by, updated_on, updated_by, message_id)   
            select acc_index,
                    get_punter_key(jt.acc_index) punter_key,
                    get_punter_id(jt.acc_index) punter_id,
                    case when max_limit_type='MAX_CASINO_PLAYTIME_LIMIT' then
                        case limit_period
                            when 'day' then 13
                            when 'week' then 14
                            when 'month' then 15
                        --else 15
                        end
                        when max_limit_type='MAX_NET_LOSS_LIMIT' then
                        case limit_period
                            when 'week' then 8 -- Förlustgräns finns bara vecka
                        --else 8
                        end
                    end bet_limit_type_id,
                    limit_value as limit_value,
                    limit_unit as unit,  
                    (select bet_vertical_id from spst.bet_vertical where code=max_limit_vertical) AS bet_vertical_id,
                    FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'UTC' valid_from_utc,
                    FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'UTC' valid_to_utc,
                    FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'Europe/Stockholm' valid_from_se,
                    FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'Europe/Stockholm' valid_to_se,
                    event_type as event_type,
                    msg_timestamp_utc as message_timestamp_utc,              
                    lv_event_type as message_type,
                    sysdate created_on,
                    coalesce(msg_created_by, 'Unknown') created_by,
                    null as updated_on,
                    null as updated_by,
                    coalesce(msg_id, p_event_type) as message_id
                from json_table (
                                p_message,
                                '$'
                                columns (msg_id varchar2(255) path '$.id',
                                         max_limit_type varchar2(255) path '$.maxLimit.type',
                                         max_limit_vertical varchar2(255) path '$.maxLimit.vertical',
                                         acc_index varchar2(255) path '$.maxLimit.accountIndex',
                                         limit_unit varchar2(255) path '$.maxLimit.unit',
                                         msg_timestamp_utc timestamp(9) path '$.timestamp',
                                         limit_value number path '$.maxLimit.limit',
                                         limit_period varchar2(255) path '$.maxLimit.period',
                                         valid_from timestamp(9) path '$.maxLimit.validFrom',
                                         valid_to timestamp(9) path '$.maxLimit.validTo',
                                         msg_created_by varchar2(255) path '$.createdBy',
                                         event_type varchar2(255) path '$.patron'
                                        )
                                ) jt
                    log errors into spst.err$_bet_limit_max_changes('load_bet_limit_max_limit_changes - '||lv_event_type) reject limit unlimited;

exception
   when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end load_bet_limit_max_limit_changes;



function get_last_run_date return date is

  lc_prc varchar2(30) := 'get_last_run_date';
  lv_max_date date := null;
  lv_return_date date := null;

begin

  lv_max_date := null;
  
  --for first load assume that we should run data for yesterday,
  --if this is not the case put the correct date into case_management_load_log manually
  select coalesce(max(run_date),trunc(sysdate-2)) into lv_max_date from bet_limit_load_log;
  
  lv_return_date:=lv_max_date;

  return lv_return_date;

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end get_last_run_date;

function date_exists (p_run_date in date) return boolean is

  date_count pls_integer;

begin

  date_count := 0;

  select count(*) into date_count from bet_limit_load_log where run_date=p_run_date;

  return (date_count>0);

end date_exists;

procedure log_date_load (p_run_date in date)
is

  lc_prc varchar2(30) := 'log_date_load';

begin

  if date_exists(p_run_date) then

     update bet_limit_load_log
        set last_load_date=sysdate
      where run_date=p_run_date;

  else

    insert into bet_limit_load_log(run_date, last_load_date)
         values(p_run_date, sysdate);

  end if;

  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end log_date_load;

procedure insert_message_history (p_message clob, p_message_type varchar2) is
  lc_prc varchar2(30) := 'insert_message_history';
begin

  insert into bet_limit_dequeue_hist (message, message_type, created_date)
       values (p_message, p_message_type, sysdate);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end insert_message_history;

procedure bet_limit_etl
is

  lc_prc varchar2(30) := 'bet_limit_etl';

  dequeue_options dbms_aq.dequeue_options_t;
  message_properties dbms_aq.message_properties_t;

  jms_text_message sys.aq$_jms_text_message;
  text_message clob; --varchar2(32767);
  system_generated_id_of_the_msg raw(16);

  exc_no_message exception;
  pragma exception_init(exc_no_message, -25228);

  exc_listenerror     exception;
  pragma              exception_init(exc_listenerror,-25254);

  lv_message_type varchar2(255);
  
  type t_msg_type is table of number index by varchar2(255);
  ll_msg_type                         t_msg_type;
  lv_msg_type                         varchar2(255);

begin
  dequeue_options.navigation := dbms_aq.first_message;
  dequeue_options.wait := 0;
  ll_msg_type.delete;

  <<inner_loop>>
  loop
    begin

      sys.dbms_aq.dequeue(
                           queue_name          => 'BET_LIMIT_INBOX_QUEUE',
                           dequeue_options     => dequeue_options,
                           message_properties  => message_properties,
                           payload             => jms_text_message,
                           msgid               => system_generated_id_of_the_msg);

      jms_text_message.get_text(text_message);

      lv_message_type := jms_text_message.get_type;
      --dbms_output.put_line ('Dequeued: '||lv_message_type);
      
      --lv_message_type := coalesce(jms_text_message.get_type,coalesce(json_value(text_message, '$.type' returning varchar2(255)), 'null'));
      if ll_msg_type.EXISTS(lv_message_type) then
         ll_msg_type(lv_message_type) := ll_msg_type(lv_message_type) + 1;
      else
         ll_msg_type(lv_message_type) := 1;
      end if;
      
      case lv_message_type

        /*when betlimitvr then
          insert_message_history(text_message,betlimitvr);
          load_bet_limit_bet_time_changes(text_message,betlimitvr);
          load_bet_limit_stake_changes(text_message);*/
        when betlimitnetlosscasinoweek then
          insert_message_history(text_message,betlimitnetlosscasinoweek);
          load_bet_limit_net_loss_changes(text_message,betlimitnetlosscasinoweek);
        when betlimitnetlosshorseweek then
          insert_message_history(text_message,betlimitnetlosshorseweek);
          load_bet_limit_net_loss_changes(text_message, betlimitnetlosshorseweek);
        when betlimitnetlosssportweek then
          insert_message_history(text_message,betlimitnetlosssportweek);
          load_bet_limit_net_loss_changes(text_message, betlimitnetlosssportweek);
        when betlimitplaytimecasinoday then
          insert_message_history(text_message,betlimitplaytimecasinoday);
          load_bet_limit_play_time_changes(text_message, betlimitplaytimecasinoday);
        when betlimitplaytimecasinoweek then
          insert_message_history(text_message,betlimitplaytimecasinoweek);
          load_bet_limit_play_time_changes(text_message, betlimitplaytimecasinoweek);
        when betlimitplaytimecasinomonth then
          insert_message_history(text_message,betlimitplaytimecasinomonth);
          load_bet_limit_play_time_changes(text_message, betlimitplaytimecasinomonth);
        when betlimitselfexclusioncasino then
          insert_message_history(text_message,betlimitselfexclusioncasino);
          load_bet_limit_self_exclusion_changes(text_message, betlimitselfexclusioncasino);
        when betlimitselfexclusionsport then
          insert_message_history(text_message,betlimitselfexclusionsport);
          load_bet_limit_self_exclusion_changes(text_message, betlimitselfexclusionsport);
        when betlimitselfexclusionhorse then
          insert_message_history(text_message,betlimitselfexclusionhorse);
          load_bet_limit_self_exclusion_changes(text_message, betlimitselfexclusionhorse);
        when betlimitdepositcreated then
          insert_message_history(text_message,betlimitdepositcreated);
          load_bet_limit_deposit_changes(text_message, betlimitdepositcreated);
        when betlimitlogintimeday then 
           insert_message_history(text_message,betlimitlogintimeday);
           load_bet_limit_login_time_changes (text_message);
        when betlimitlogintimeweek then 
           insert_message_history(text_message,betlimitlogintimeweek);
           load_bet_limit_login_time_changes (text_message);
        when betlimitlogintimemonth then 
           insert_message_history(text_message,betlimitlogintimemonth);
           load_bet_limit_login_time_changes (text_message);
        when betlimitdepositmaxlimit then -- Max limit insättningsgräns (payment)
           insert_message_history(text_message,betlimitdepositmaxlimit);
           load_bet_limit_deposit_max_limit_changes (text_message, betlimitdepositmaxlimit);
        when betlimitsetmaxlimit then -- Max limit SET Speltidsgräns och förlustgräns (rgs)
           insert_message_history(text_message,betlimitsetmaxlimit);
           load_bet_limit_max_limit_changes (text_message, betlimitsetmaxlimit);
        when betlimitdeletemaxlimit then -- Max limit DELETE Speltidsgräns och förlustgräns (rgs)
           insert_message_history(text_message,betlimitdeletemaxlimit);
           load_bet_limit_max_limit_changes (text_message, betlimitdeletemaxlimit);
        /*when betlimitdepositcustomeractivated then
          insert_message_history(text_message,betlimitdepositcustomeractivated);
          load_bet_limit_deposit_changes(text_message, betlimitdepositcustomeractivated);
        when betlimitdepositdeleted then
          insert_message_history(text_message,betlimitdepositdeleted);
          load_bet_limit_deposit_changes(text_message, betlimitdepositdeleted);*/
        else
          insert_message_history(text_message,coalesce(lv_message_type,'null'));
      end case;

    exception
      when exc_no_message then
        exit inner_loop;
      when exc_listenerror then
          exit inner_loop;
      when others then
        raise;
    end;
  end loop;

  lv_msg_type := ll_msg_type.FIRST;         
  while lv_msg_type is not null
  loop
     dbms_output.put_line (lv_msg_type||': '||ll_msg_type(lv_msg_type));
     lv_msg_type := ll_msg_type.NEXT (lv_msg_type);
  end loop;
  ll_msg_type.delete;
exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end bet_limit_etl;

procedure create_files(p_run_date in date) is

  lc_prc varchar2(30) := 'create_files';
  lv_application filgenerering.applikation%type := 'BET_LIMIT';

begin
  
  --Självavstängningar till Adobe
  skapa_filer.skapa_filer (pin_applikation => lv_application,
                           laddatum        => to_char(p_run_date,'yyyymmdd'),
                           parametervarde  => to_char(p_run_date,'yyyymmdd'),
                           pin_objektnamn  => 'SELF_EXCLUSION_CRM_VIEW' 
                           );
    
exception
  when others then
    utilities.log_error(gc_job_name, gc_package, lc_prc);
    raise;

end create_files;

procedure workshop_missing_punter is
  lc_prc varchar2(30) := 'workshop_missing_punter';
  v_punter_key number;
  v_punter_id number;
  
begin
     --fetch new punters again
     stat_kontoadm_etl.initiate_punter_load (user||'.'||gc_package||'.'||lc_prc);
     
     for rec in ( select rowid,
                  acc_index,
                  bet_limit_type_id,
                  limit_value,
                  bet_vertical_id,                  
                  to_timestamp(valid_from) valid_from,
                  to_timestamp(valid_to) valid_to,
                  to_timestamp(updated_on) updated_on,
                  updated_by,
                  to_timestamp(message_timestamp) message_timestamp,
                  message_type,
                  source_system,
                  spst_created_date
             from spst.err$_bet_limit_deposit_changes
                where ora_err_number$=1400
                  and punter_key is null
                  and punter_id is null) loop

    v_punter_key:=bet_limit_job.get_punter_key(rec.acc_index);
    v_punter_id:=bet_limit_job.get_punter_id(rec.acc_index);
    
    if v_punter_key is not null and v_punter_id is not null then
              
    insert into spst.bet_limit_deposit_changes (acc_index, punter_key, punter_id, bet_limit_type_id, limit_value, bet_vertical_id, valid_from, valid_to, updated_on, updated_by, message_timestamp, message_type, source_system, spst_created_date)
    values
    (rec.acc_index, v_punter_key, v_punter_id, rec.bet_limit_type_id, rec.limit_value, rec.bet_vertical_id, rec.valid_from, rec.valid_to, rec.updated_on, rec.updated_by, rec.message_timestamp, rec.message_type, rec.source_system, rec.spst_created_date);
    
        if sql%rowcount > 0 then 
        
        delete from spst.err$_bet_limit_deposit_changes where rowid = rec.rowid;
        
        dbms_output.put_line('bet_limit_deposit_changes - Repair complete for acc_index: '||rec.acc_index);
        
        end if;
   
    end if;
    
    end loop;
     
exception
  when others then
    utilities.log_error(gc_job_name, gc_package, lc_prc);
    raise;
end workshop_missing_punter;

procedure check_error_message(p_table in varchar2)
is

  lc_prc varchar2(30) := 'check_error_message';
  exc_error_found  exception;
  exc_no_table  exception;
  lv_no_of_error_message number;

begin

  case p_table
    /*when betlimitbettime then
      delete from spst.err$_bet_limit_bet_time_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_bet_time_changes;
    when betlimitstake then
      delete from spst.err$_bet_limit_stake_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_stake_changes;*/
    when betlimitnetloss then
      delete from spst.err$_bet_limit_net_loss_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_net_loss_changes;
     when betlimitplaytime then
      delete from spst.err$_bet_limit_play_time_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_play_time_changes;
     when betlimitlogintime then
      delete from spst.err$_bet_limit_login_time_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_login_time_changes;
     when betlimitselfexclusion then
      delete from spst.err$_bet_limit_self_exclusion_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_self_exclusion_changes;
     when betlimitdeposit then
      delete from spst.err$_bet_limit_deposit_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_deposit_changes;
       if lv_no_of_error_message > 0 then --try to repair
          workshop_missing_punter;
          select count(*) into lv_no_of_error_message
          from spst.err$_bet_limit_deposit_changes;
       end if;
     when betlimitdepositmaxlimit then
      delete from spst.err$_bet_limit_max_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_max_changes;     
     when betlimitsetmaxlimit then
      delete from spst.err$_bet_limit_max_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_max_changes;
     when betlimitdeletemaxlimit then
      delete from spst.err$_bet_limit_max_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_bet_limit_max_changes;    
    else
      raise exc_no_table;
    end case;

  if lv_no_of_error_message > 0 then
    raise exc_error_found;
  end if;

exception
  when exc_error_found then
    raise_application_error (-20002, 'Error in bet limit error table ' || p_table || ' was found!');
  when exc_no_table then
    raise_application_error (-20003, 'A table name that could not be handled was found!');
  when others then
    utilities.log_error(gc_job_name, gc_package, lc_prc);
    raise;
end check_error_message;

/*procedure insert_bet_limit_bet_time_current (p_load_date in date,p_bet_limit_type_id in number, p_bet_vertical_id in number) is
lc_prc                                 varchar2(50) := 'insert_bet_limit_bet_time_current';
ln_batch_log_id                        batch_log.id%type;
ld_start_time                          date := sysdate;
ln_rows                                number := 0;

begin
   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => null,
                            pio_id           => ln_batch_log_id);                     
                            
   insert into spst.bet_limit_bet_time_current (time_key, 
                                                punter_key, 
                                                punter_id, 
                                                acc_index, 
                                                bet_limit_type_id, 
                                                bet_limit_change_id,
                                                amount,
                                                amount_change,
                                                bet_vertical_id,
                                                valid_from,
                                                valid_to,
                                                lock_date)
   select time_key,
          punter_key,
          punter_id,
          acc_index,
          bet_limit_type_id,
          bet_limit_change_id,
          amount,
          amount_change,
          bet_vertical_id,
          valid_from,
          valid_to,
          lock_date
   from (
     (select  to_char(p_load_date,'yyyymmdd') time_key,
              acc_index,
              punter_key,
              get_punter_id(acc_index) punter_id,
              bet_limit_type_id,
              bet_vertical_id,
              amount,
              valid_from,
              valid_to,
              lock_date,
              nvl(amount_change,0) amount_change,
              case 
                --om förändring är null så fanns inte kunden sedan innan, dvs. ny gräns och id = 0
                when amount_change is null then 0---1
                --Om förändring är minus så sänkning av gräns, dvs id=3
                when amount_change < 0 then 3
                --Om förändring är 0 så oförändrad gräns, dvs id=1			
                when amount_change = 0 then 1
                --Om förändring är plus så höjning av gräns, dvs id=2		
                when amount_change > 0 then 2
				end bet_limit_change_id,
              r
        from (select a.acc_index,
                     a.punter_key,
                     a.bet_limit_type_id,
                     a.bet_vertical_id,
                     a.amount,
                     a.amount-b.amount amount_change,
                     a.valid_from,
                     a.valid_to,
                     a.lock_date,
	  		         row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.valid_from desc, a.updated_date desc) r
			    from spst.bet_limit_bet_time_changes a, spst.bet_limit_bet_time_current b
			   where a.punter_key = b.punter_key(+)
			     and a.bet_limit_type_id = p_bet_limit_type_id
			     and a.bet_vertical_id = p_bet_vertical_id
			     and a.bet_limit_type_id=b.bet_limit_type_id(+)
			     and a.bet_vertical_id=b.bet_vertical_id(+)
			     and b.time_key_date(+) = p_load_date-1
		         and trunc(a.valid_from) <= p_load_date
			     and (trunc(a.valid_to) is null or trunc(a.valid_to) >= p_load_date)) x
	   where x.r = 1));

   ln_rows := sql%rowcount;

   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => ln_rows,
                            pio_id           => ln_batch_log_id);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end insert_bet_limit_bet_time_current;*/

/*procedure insert_bet_limit_stake_current (p_load_date in date,p_bet_limit_type_id in number, p_bet_vertical_id in number) is
lc_prc                                 varchar2(50) := 'insert_bet_limit_stake_current';
ln_batch_log_id                        batch_log.id%type;
ld_start_time                          date := sysdate;
ln_rows                                number := 0;

begin
   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => null,
                            pio_id           => ln_batch_log_id);                     
                            
   insert into spst.bet_limit_stake_current (time_key, 
                                                punter_key, 
                                                punter_id, 
                                                acc_index, 
                                                bet_limit_type_id, 
                                                bet_limit_change_id,
                                                amount,
                                                amount_change,
                                                bet_vertical_id,
                                                valid_from,
                                                valid_to,
                                                lock_date)
      select time_key,
          punter_key,
          punter_id,
          acc_index,
          bet_limit_type_id,
          bet_limit_change_id,
          amount,
          amount_change,
          bet_vertical_id,
          valid_from,
          valid_to,
          lock_date
   from (
     (select  to_char(p_load_date,'yyyymmdd') time_key,
              acc_index,
              punter_key,
              get_punter_id(acc_index) punter_id,
              bet_limit_type_id,
              bet_vertical_id,
              amount,
              valid_from,
              valid_to,
              lock_date,
              nvl(amount_change,0) amount_change,
              case 
                --om förändring är null så fanns inte kunden sedan innan, dvs. ny gräns och id = 0
                when amount_change is null then 0---1
                --Om förändring är minus så sänkning av gräns, dvs id=3
                when amount_change < 0 then 3
                --Om förändring är 0 så oförändrad gräns, dvs id=1			
                when amount_change = 0 then 1
                --Om förändring är plus så höjning av gräns, dvs id=2		
                when amount_change > 0 then 2
				end bet_limit_change_id,
              r
        from (select a.acc_index,
                     a.punter_key,
                     a.bet_limit_type_id,
                     a.bet_vertical_id,
                     a.amount,
                     a.amount-b.amount amount_change,
                     a.valid_from,
                     a.valid_to,
                     a.lock_date,
	  		         row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.valid_from desc, a.updated_date desc) r
			    from spst.bet_limit_stake_changes a, spst.bet_limit_stake_current b
			   where a.punter_key = b.punter_key(+)
			     and a.bet_limit_type_id = p_bet_limit_type_id
			     and a.bet_vertical_id = p_bet_vertical_id
			     and a.bet_limit_type_id=b.bet_limit_type_id(+)
			     and a.bet_vertical_id=b.bet_vertical_id(+)
			     and b.time_key_date(+) = p_load_date-1
		         and trunc(a.valid_from) <= p_load_date
			     and (trunc(a.valid_to) is null or trunc(a.valid_to) >= p_load_date)) x
	   where x.r = 1));

   ln_rows := sql%rowcount;

   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => ln_rows,
                            pio_id           => ln_batch_log_id);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end insert_bet_limit_stake_current;*/

procedure insert_bet_limit_play_time_current (p_load_date in date,p_bet_limit_type_id in number, p_bet_vertical_id in number) is
lc_prc                                 varchar2(50) := 'insert_bet_limit_play_time_current';
ln_batch_log_id                        batch_log.id%type;
ld_start_time                          date := sysdate;
ln_rows                                number := 0;

begin
   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => null,
                            pio_id           => ln_batch_log_id);                     
                            
   insert into spst.bet_limit_play_time_current (time_key, 
                                                punter_key, 
                                                punter_id, 
                                                acc_index, 
                                                bet_limit_type_id, 
                                                bet_limit_change_id,
                                                limit_value,
                                                limit_value_change,
                                                bet_vertical_id,
                                                valid_from,
                                                valid_to)
      select time_key,
          punter_key,
          punter_id,
          acc_index,
          bet_limit_type_id,
          bet_limit_change_id,
          limit_value,
          limit_value_change,
          bet_vertical_id,
          valid_from,
          valid_to
   from (
     (select  to_char(p_load_date,'yyyymmdd') time_key,
              acc_index,
              punter_key,
              get_punter_id(acc_index) punter_id,
              bet_limit_type_id,
              bet_vertical_id,
              limit_value,
              valid_from,
              valid_to,
              nvl(limit_value_change,0) limit_value_change,
              case 
                --om förändring är null så fanns inte kunden sedan innan, dvs. ny gräns och id = 0
                when limit_value_change is null then 0---1
                --Om förändring är minus så sänkning av gräns, dvs id=3
                when limit_value_change < 0 then 3
                --Om förändring är 0 så oförändrad gräns, dvs id=1			
                when limit_value_change = 0 then 1
                --Om förändring är plus så höjning av gräns, dvs id=2		
                when limit_value_change > 0 then 2
				end bet_limit_change_id,
              r
        from (select a.acc_index,
                     a.punter_key,
                     a.bet_limit_type_id,
                     a.bet_vertical_id,
                     a.limit_value,
                     a.limit_value-b.limit_value limit_value_change,
                     a.valid_from,
                     a.valid_to,
	  		         row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.valid_from desc, a.updated_date desc) r
			    from spst.bet_limit_play_time_changes a, spst.bet_limit_play_time_current b
			   where a.punter_key = b.punter_key(+)
			     and a.bet_limit_type_id = p_bet_limit_type_id
			     and a.bet_vertical_id = p_bet_vertical_id
			     and a.bet_limit_type_id=b.bet_limit_type_id(+)
			     and a.bet_vertical_id=b.bet_vertical_id(+)
			     and b.time_key_date(+) = p_load_date-1
		         and trunc(a.valid_from) <= p_load_date
			     and (trunc(a.valid_to) is null or trunc(a.valid_to) >= p_load_date)) x
	   where x.r = 1));

   ln_rows := sql%rowcount;

   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => ln_rows,
                            pio_id           => ln_batch_log_id);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end insert_bet_limit_play_time_current;

procedure insert_bet_limit_net_loss_current (p_load_date in date,p_bet_limit_type_id in number, p_bet_vertical_id in number) is
lc_prc                                 varchar2(50) := 'insert_bet_limit_net_loss_current';
ln_batch_log_id                        batch_log.id%type;
ld_start_time                          date := sysdate;
ln_rows                                number := 0;

begin
  utilities.reg_batch_log( pin_package      => gc_package,
                           pin_procedure    => lc_prc,
                           pin_started_on   => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows      => null,
                           pio_id           => ln_batch_log_id);                     
  
  --Casino är obligatorisk vilket gör att vi inte behöver hantera valid_to utan då enbart kan sortera på valid_from för att veta vilken gräns som gäller 
  if p_bet_vertical_id = 3 then
                         
    insert into spst.bet_limit_net_loss_current (time_key, 
                                                 punter_key, 
                                                 punter_id, 
                                                 acc_index, 
                                                 bet_limit_type_id, 
                                                 bet_limit_change_id,
                                                 limit_value,
                                                 limit_value_change,
                                                 bet_vertical_id,
                                                 valid_from,
                                                 valid_to)
      select time_key,
             punter_key,
             punter_id,
             acc_index,
             bet_limit_type_id,
             bet_limit_change_id,
             limit_value,
             limit_value_change,
             bet_vertical_id,
             valid_from,
             valid_to
        from ((select to_char(p_load_date,'yyyymmdd') time_key,
                              acc_index,
                              punter_key,
                              get_punter_id(acc_index) punter_id,
                              bet_limit_type_id,
                              bet_vertical_id,
                              limit_value,
                              valid_from,
                              valid_to,
                              nvl(limit_value_change,0) limit_value_change,
                              case 
                                --om förändring är null så fanns inte kunden sedan innan, dvs. ny gräns och id = 0
                                when limit_value_change is null then 0---1
                                --Om förändring är minus så sänkning av gräns, dvs id=3
                                when limit_value_change < 0 then 3
                                --Om förändring är 0 så oförändrad gräns, dvs id=1			
                                when limit_value_change = 0 then 1
                                --Om förändring är plus så höjning av gräns, dvs id=2		
                                when limit_value_change > 0 then 2
                              end bet_limit_change_id,
                              r
                        from (select a.acc_index,
                                     a.punter_key,
                                     a.bet_limit_type_id,
                                     a.bet_vertical_id,
                                     a.limit_value,
                                     a.limit_value-b.limit_value limit_value_change,
                                     a.valid_from,
                                     a.valid_to,
                                     row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.valid_from desc, a.updated_date desc) r
                                from spst.bet_limit_net_loss_changes a, spst.bet_limit_net_loss_current b
                               where a.punter_key = b.punter_key(+)
                                 and a.bet_limit_type_id = p_bet_limit_type_id
                                 and a.bet_vertical_id = p_bet_vertical_id
                                 and a.bet_limit_type_id=b.bet_limit_type_id(+)
                                 and a.bet_vertical_id=b.bet_vertical_id(+)
                                 and b.time_key_date(+) = p_load_date-1
                                 and trunc(a.valid_from) <= p_load_date
                                 and (trunc(a.valid_to) is null or trunc(a.valid_to) >= p_load_date)) x
                       where x.r = 1));

  end if;	     
  
  --Häst och Sport är frivillig vilket gör det mer komplext i RGS (man kan ta bort gränser i RGS som i meddelandena till oss inte får något valid_to viket gör
  --att vi kan få gränser som ser ut att gälla med ett valid_from som är mindre än ett valid_to på en tidigare gräns). Detta gör att vi måste titta på det 
  --senaste meddelandet från RGS (updated_date = timestamp i meddelandet från RGS) för att veta vilken gräns som gäller. Sedan måste vi även kolla att den senaste 
  --gränsen inte har ett valid_to.     
  if p_bet_vertical_id in (1,4) then
  	   
    insert into spst.bet_limit_net_loss_current (time_key, 
                                                 punter_key, 
                                                 punter_id, 
                                                 acc_index, 
                                                 bet_limit_type_id, 
                                                 bet_limit_change_id,
                                                 limit_value,
                                                 limit_value_change,
                                                 bet_vertical_id,
                                                 valid_from,
                                                 valid_to)
      select time_key,
             punter_key,
             punter_id,
             acc_index,
             bet_limit_type_id,
             bet_limit_change_id,
             limit_value,
             limit_value_change,
             bet_vertical_id,
             valid_from,
             valid_to
      from (
        (select  to_char(p_load_date,'yyyymmdd') time_key,
                 acc_index,
                 punter_key,
                 get_punter_id(acc_index) punter_id,
                 bet_limit_type_id,
                 bet_vertical_id,
                 limit_value,
                 valid_from,
                 valid_to,
                 nvl(limit_value_change,0) limit_value_change,
                 case 
                   --om förändring är null så fanns inte kunden sedan innan, dvs. ny gräns och id = 0
                   when limit_value_change is null then 0---1
                   --Om förändring är minus så sänkning av gräns, dvs id=3
                   when limit_value_change < 0 then 3
                   --Om förändring är 0 så oförändrad gräns, dvs id=1			
                   when limit_value_change = 0 then 1
                   --Om förändring är plus så höjning av gräns, dvs id=2		
                   when limit_value_change > 0 then 2
                   end bet_limit_change_id,
                 r
           from (select a.acc_index,
                        a.punter_key,
                        a.bet_limit_type_id,
                        a.bet_vertical_id,
                        a.limit_value,
                        a.limit_value-b.limit_value limit_value_change,
                        a.valid_from,
                        a.valid_to,
                        row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.updated_date desc, a.valid_from desc) r
                   from spst.bet_limit_net_loss_changes a, spst.bet_limit_net_loss_current b
                  where a.punter_key = b.punter_key(+)
                    and a.bet_limit_type_id = p_bet_limit_type_id
                    and a.bet_vertical_id = p_bet_vertical_id
                    and a.bet_limit_type_id=b.bet_limit_type_id(+)
                    and a.bet_vertical_id=b.bet_vertical_id(+)
                    and b.time_key_date(+) = p_load_date-1
                    and trunc(a.valid_from) <= p_load_date) x
          where x.r = 1
          and (trunc(valid_to) is null or trunc(valid_to) >= p_load_date)));
          
  end if;
          
  ln_rows := sql%rowcount;

  utilities.reg_batch_log( pin_package      => gc_package,
                           pin_procedure    => lc_prc,
                           pin_started_on   => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows      => ln_rows,
                           pio_id           => ln_batch_log_id);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end insert_bet_limit_net_loss_current;

procedure insert_bet_limit_self_exclusion_current (p_load_date in date) is
lc_prc                                 varchar2(50) := 'insert_bet_limit_self_exclusion_current';
ln_batch_log_id                        batch_log.id%type;
ld_start_time                          date := sysdate;
ln_rows                                number := 0;

begin
  utilities.reg_batch_log( pin_package      => gc_package,
                           pin_procedure    => lc_prc,
                           pin_started_on   => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows      => null,
                           pio_id           => ln_batch_log_id);                       
  	   
    insert into spst.bet_limit_self_exclusion_current (time_key, 
                                                       punter_key, 
                                                       punter_id, 
                                                       acc_index, 
                                                       bet_limit_type_id,
                                                       limit_duration,
                                                       limit_kind,
                                                       temporary_exclusion,
                                                       bet_vertical_id,
                                                       valid_from,
                                                       valid_to,
                                                       excluded_until,
                                                       locked_until)
            select time_key, 
                   punter_key, 
                   punter_id, 
                   acc_index, 
                   bet_limit_type_id,
                   limit_duration,
                   limit_kind,
                   temporary_exclusion,
                   bet_vertical_id,
                   valid_from,
                   valid_to,
                   excluded_until,
                   locked_until
            from (
              (select  to_char(p_load_date,'yyyymmdd') time_key,
                       acc_index,
                       punter_key,
                       get_punter_id(acc_index) punter_id,
                       bet_limit_type_id,
                       round(limit_duration) limit_duration,
                       limit_kind,
                       temporary_exclusion,
                       bet_vertical_id,
                       valid_from,
                       valid_to,
                       excluded_until,
                       locked_until,
                       r
                 from (select a.acc_index,
                              a.punter_key,
                              a.bet_limit_type_id,
                              a.limit_duration,
                              a.limit_kind,
                              a.temporary_exclusion,
                              a.bet_vertical_id,
                              a.valid_from,
                              a.valid_to,
                              a.excluded_until,
                              a.locked_until,
                              row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.valid_from desc, a.updated_date desc) r
                         from spst.bet_limit_self_exclusion_changes a
                        where trunc(a.valid_from) <= p_load_date) x
                where x.r = 1
                and (trunc(valid_to) is null or trunc(valid_to) >= p_load_date)));
        
  ln_rows := sql%rowcount;

  utilities.reg_batch_log( pin_package      => gc_package,
                           pin_procedure    => lc_prc,
                           pin_started_on   => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows      => ln_rows,
                           pio_id           => ln_batch_log_id);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end insert_bet_limit_self_exclusion_current;

procedure insert_bet_limit_deposit_current (p_load_date in date,p_bet_limit_type_id in number, p_bet_vertical_id in number) is
lc_prc                                 varchar2(50) := 'insert_bet_limit_deposit_current';
ln_batch_log_id                        batch_log.id%type;
ld_start_time                          date := sysdate;
ln_rows                                number := 0;

begin
  utilities.reg_batch_log( pin_package      => gc_package,
                           pin_procedure    => lc_prc,
                           pin_started_on   => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows      => null,
                           pio_id           => ln_batch_log_id);                     
                         
  insert into spst.bet_limit_deposit_current (time_key, 
                                               punter_key, 
                                               punter_id, 
                                               acc_index, 
                                               bet_limit_type_id, 
                                               bet_limit_change_id,
                                               limit_value,
                                               limit_value_change,
                                               bet_vertical_id,
                                               valid_from,
                                               valid_to)
    select time_key,
           punter_key,
           punter_id,
           acc_index,
           bet_limit_type_id,
           bet_limit_change_id,
           limit_value,
           limit_value_change,
           bet_vertical_id,
           valid_from,
           valid_to
      from ((select to_char(p_load_date,'yyyymmdd') time_key,
                            acc_index,
                            punter_key,
                            get_punter_id(acc_index) punter_id,
                            bet_limit_type_id,
                            bet_vertical_id,
                            limit_value,
                            valid_from,
                            valid_to,
                            nvl(limit_value_change,0) limit_value_change,
                            case 
                              --om förändring är null så fanns inte kunden sedan innan, dvs. ny gräns och id = 0
                              when limit_value_change is null then 0---1
                              --Om förändring är minus så sänkning av gräns, dvs id=3
                              when limit_value_change < 0 then 3
                              --Om förändring är 0 så oförändrad gräns, dvs id=1			
                              when limit_value_change = 0 then 1
                              --Om förändring är plus så höjning av gräns, dvs id=2		
                              when limit_value_change > 0 then 2
                            end bet_limit_change_id,
                            r
                      from (select a.acc_index,
                                   a.punter_key,
                                   a.bet_limit_type_id,
                                   a.bet_vertical_id,
                                   a.limit_value,
                                   a.limit_value-b.limit_value limit_value_change,
                                   a.valid_from,
                                   a.valid_to,
                                   row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.message_timestamp desc, a.valid_from desc) r
                              from spst.bet_limit_deposit_changes a, spst.bet_limit_deposit_current b
                             where a.punter_key = b.punter_key(+)
                               and a.bet_limit_type_id = p_bet_limit_type_id
                               and a.bet_vertical_id = p_bet_vertical_id
                               and a.bet_limit_type_id=b.bet_limit_type_id(+)
                               and a.bet_vertical_id=b.bet_vertical_id(+)
                               --and lower(a.message_type) = 'deposit-limit'
                               and b.time_key_date(+) = p_load_date-1
                               and trunc(a.valid_from) <= p_load_date
                               and (trunc(a.valid_to) is null or trunc(a.valid_to) >= p_load_date)) x
                     where x.r = 1));     
          
  ln_rows := sql%rowcount;

  utilities.reg_batch_log( pin_package      => gc_package,
                           pin_procedure    => lc_prc,
                           pin_started_on   => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows      => ln_rows,
                           pio_id           => ln_batch_log_id);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end insert_bet_limit_deposit_current;

procedure insert_bet_limit_login_time_current (p_load_date in date,p_bet_limit_type_id in number, p_bet_vertical_id in number) is
lc_prc                                 varchar2(50) := 'insert_bet_limit_login_time_current';
ln_batch_log_id                        batch_log.id%type;
ld_start_time                          date := sysdate;
ln_rows                                number := 0;

begin
   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => null,
                            pio_id           => ln_batch_log_id);                     
                          
   insert into spst.bet_limit_login_time_current (limit_id,
                                                  time_key, 
                                                  punter_key, 
                                                  punter_id, 
                                                  acc_index, 
                                                  bet_limit_type_id, 
                                                  bet_limit_change_id,
                                                  limit_value,
                                                  limit_value_change,
                                                  bet_vertical_id,
                                                  valid_from,
                                                  valid_to)
      select limit_id,
             time_key,
             punter_key,
             get_punter_id(acc_index) punter_id,
             acc_index,
             bet_limit_type_id,
             bet_limit_change_id,
             limit_value,
             limit_value_change,
             bet_vertical_id,
             valid_from,
             valid_to
        from ((select limit_id,
                      to_char(p_load_date,'yyyymmdd') time_key,
                      acc_index,
                      punter_key,
                      bet_limit_type_id,
                      bet_vertical_id,
                      limit_value,
                      valid_from,
                      valid_to,
                      limit_value_change,
                      case 
                        --om förändring är null så fanns inte kunden sedan innan, dvs. ny gräns och id = 0
                        when limit_value_change is null then 0---1
                        --Om förändring är minus så sänkning av gräns, dvs id=3
                        when limit_value_change < 0 then 3
                        --Om förändring är 0 så oförändrad gräns, dvs id=1			
                        when limit_value_change = 0 then 1
                        --Om förändring är plus så höjning av gräns, dvs id=2		
                        when limit_value_change > 0 then 2
                      end bet_limit_change_id,
                      r
                from (select a.limit_id,
                             a.acc_index,
                             a.punter_key,
                             a.bet_limit_type_id,
                             a.bet_vertical_id,
                             a.limit_value,
                             a.limit_value - b.limit_value limit_value_change,
                             a.valid_from,
                             a.valid_to,
                             row_number() over (partition by a.punter_key, a.bet_limit_type_id, a.bet_vertical_id order by a.msg_timestamp_utc desc, a.valid_from desc) r
                        from spst.bet_limit_login_time_changes a, 
                             spst.bet_limit_login_time_current b
                       where a.punter_key         = b.punter_key(+)
                         and a.bet_limit_type_id  = b.bet_limit_type_id(+)
                         --and a.bet_vertical_id    = b.bet_vertical_id(+) -- vertical is always 0 = All.
                         and b.time_key_date(+)   = p_load_date-1
                         and trunc(a.valid_from) <= p_load_date
                         and a.bet_limit_type_id  = p_bet_limit_type_id
                         --and a.bet_vertical_id    = p_bet_vertical_id    -- vertical is always 0 = All.
                         and (trunc(a.valid_to) is null or trunc(a.valid_to) >= p_load_date)) x
               where x.r = 1));
           
   ln_rows := sql%rowcount;
 
   utilities.reg_batch_log( pin_package      => gc_package,
                            pin_procedure    => lc_prc,
                            pin_started_on   => ld_start_time,
                            pin_step_in_proc => 1,
                            pin_no_rows      => ln_rows,
                            pio_id           => ln_batch_log_id);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end insert_bet_limit_login_time_current;

procedure do
is

  l_str	varchar2(255);
  lc_prc varchar2(30) := 'do';

  lv_last_run_date date;
  lv_current_run_date date;
  lv_end_date date;

begin

  stat_kontoadm_etl.initiate_punter_load (user||'.'||gc_package||'.'||lc_prc);
  staging_code.time_fill_day_interval;

  --För att få med klockslag till error-tabellerna på datum-kolumnerna
/*  l_str := 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD HH24:MI:SS''';
  execute immediate l_str;
*/
  
  execute immediate q'c alter session set NLS_TERRITORY = 'SWEDEN'c';
  
  execute immediate q'c alter session set NLS_LANGUAGE = 'SWEDISH'c';
  
  execute immediate q'c alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'c';
    
  execute immediate q'c alter session set TIME_ZONE = 'Europe/Stockholm'c';

  bet_limit_etl;

  commit;

  --Kolla så att inte några fel hamnat i error-tabellen innan jobbet går vidare.
  --Om fel hittas stannar jobbet. Gör då följande:
  --1. Fixa felet och inserta sedan raden manuellt från error-tabellen till aktuell måltabell
  --2. Ta bort raden från error-tabellen
  --3. Starta om jobbet.
  --check_error_message(betlimitstake);
  --check_error_message(betlimitbettime);
  check_error_message(betlimitnetloss);
  check_error_message(betlimitplaytime);
  check_error_message(betlimitselfexclusion);
  check_error_message(betlimitdeposit);
  check_error_message(betlimitlogintime);

  -- Lägg till checkar på max limit !!!
  check_error_message(betlimitdepositmaxlimit);
  check_error_message(betlimitsetmaxlimit);
  check_error_message(betlimitdeletemaxlimit);
  
  lv_last_run_date:= get_last_run_date;
  lv_end_date:=trunc(sysdate-1);

  lv_current_run_date:=lv_last_run_date+1;

  while lv_current_run_date <=  lv_end_date loop
  
    --insert_bet_limit_bet_time_current (lv_current_run_date,3,2); --speltidsgräns, VR
    
    --insert_bet_limit_stake_current (lv_current_run_date,4,2); --insatsgräns, VR
    
    
    insert_bet_limit_net_loss_current(lv_current_run_date, 8, 3); --förlustgräns Casino    
    insert_bet_limit_net_loss_current(lv_current_run_date, 8, 1); --förlustgräns Häst    
    insert_bet_limit_net_loss_current(lv_current_run_date, 8, 4); --förlustgräns Sport

    ----------------------
  
    insert_bet_limit_play_time_current(lv_current_run_date, 13, 3); --speltidsgräns dag Casino
    insert_bet_limit_play_time_current(lv_current_run_date, 14, 3); --speltidsgräns vecka Casino
    insert_bet_limit_play_time_current(lv_current_run_date, 15, 3); --speltidsgräns månad Casino
    
    ----------------------
    
    insert_bet_limit_deposit_current(lv_current_run_date, 5, 0); --insättningsgräns dag alla vertikaler
    insert_bet_limit_deposit_current(lv_current_run_date, 6, 0); --insättningsgräns vecka alla vertikaler
    insert_bet_limit_deposit_current(lv_current_run_date, 7, 0); --insättningsgräns månad alla vertikaler
    
    ----------------------
    
    insert_bet_limit_self_exclusion_current(lv_current_run_date); --självavstängningar (casino,sport,häst)
        
    ----------------------
    
    insert_bet_limit_login_time_current(lv_current_run_date, 10, 0); -- inloggningstidgräns dag alla vertikaler
    insert_bet_limit_login_time_current(lv_current_run_date, 11, 0); -- inloggningstidgräns vecka alla vertikaler
    insert_bet_limit_login_time_current(lv_current_run_date, 12, 0); -- inloggningstidgräns månad alla vertikaler
    
    ----------------------

    create_files(lv_current_run_date); --fil behöver skapas till Adobe på självavstängningarna
    
    log_date_load(lv_current_run_date);

    lv_current_run_date:=lv_current_run_date+1;
    
    insert into dmspst.laddlogg (loaddate, klar, metaapp)
         values (lv_current_run_date-1,'J','BET_LIMIT');

  end loop;
  

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end do;

end bet_limit_job;
