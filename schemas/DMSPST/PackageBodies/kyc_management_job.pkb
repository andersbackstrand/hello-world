CREATE OR REPLACE PACKAGE BODY kyc_management_job as

  gc_job_name constant varchar2(30) := 'kyc_management_scheduler_job';
  gc_package constant varchar2(30) := 'kyc_management_job';
  
  --Strängar för att kolla felloggs-tabellerna samt för hantering av filskick och loggning
  kycriskgroup constant varchar2(512) := 'KYC_MANAGEMENT_CUSTOMER_RISK_GROUP';
  kycblockstatus constant varchar2(512) := 'KYC_MANAGEMENT_CUSTOMER_BLOCK_STATUS';
  kycamlblockchanges constant varchar2(512) := 'KYC_MANAGEMENT_CUSTOMER_AML_BLOCK_CHANGES';
  kyccommunicationchanges constant varchar2(512) := 'KYC_MANAGEMENT_CUSTOMER_COMMUNICATION_CHANGES';

function get_punter_id (p_account_index number) return number result_cache is

pragma udf;

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

procedure load_risk_group_changes(p_message clob)
is

  lc_prc varchar2(50) := 'load_risk_group_changes';

begin

  insert into spst.kyc_management_customer_risk_group_changes ( acc_index, 
                                                      punter_key, 
                                                      punter_id,
                                                      acc_number,
                                                      change_valid_from,
                                                      change_valid_to,
                                                      current_gambling_amount,
                                                      risk_group,
                                                      published_at,
                                                      message_type,
                                                      created_on )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              acc_number,
              substr((replace(change_valid_from,'T',' ')),1,19) change_valid_from,
              substr((replace(change_valid_to,'T',' ')),1,19) change_valid_to,
              current_gambling_amount,
              risk_group,
              CAST (FROM_TZ (published_at, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) published_at,--substr((replace(published_at,'T',' ')),1,19) published_at,
              message_type,
              sysdate created_on
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   acc_number varchar2(255) path '$.accountNumber',
                                   change_valid_from varchar2(255) path '$.changeValidFrom',
                                   change_valid_to varchar2(255) path '$.changeValidTo',
                                   current_gambling_amount varchar2(255) path '$.currentGamblingAmount',
                                   risk_group varchar2(255) path '$.riskGroup',
                                   published_at timestamp(9) path '$.publishedAt',
                                   message_type varchar2(255) path '$.type'
                                                                      )
                         ) jt
         log errors into spst.err$_kyc_management_customer_risk_group_changes('spst.kyc_management_customer_risk_group_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_risk_group_changes;

procedure load_block_status_changes(p_message clob)
is

  lc_prc varchar2(50) := 'load_block_status_changes';

begin

  insert into spst.kyc_management_customer_block_status_changes ( acc_index, 
                                                                  punter_key, 
                                                                  punter_id,
                                                                  acc_number,
                                                                  questionnaire_date,
                                                                  status_reason,
                                                                  status,
                                                                  status_changed_at,
                                                                  published_at,
                                                                  message_type,
                                                                  created_on )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              acc_number,
              substr((replace(questionnaire_date,'T',' ')),1,19) questionnaire_date,
              status_reason,
              status,
              CAST (FROM_TZ (status_changed_at, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) status_changed_at,
              CAST (FROM_TZ (published_at, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) published_at,
              message_type,
              sysdate created_on
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (
                                   acc_index varchar2(255) path '$.accountIndex',
                                   acc_number varchar2(255) path '$.accountNumber',
                                   questionnaire_date varchar2(255) path '$.date',
                                   status_reason varchar2(255) path '$.reason',
                                   status varchar2(255) path '$.status',
                                   status_changed_at timestamp(9) path '$.statusChangedAt',
                                   published_at timestamp(9) path '$.publishedAt',
                                   message_type varchar2(255) path '$.type'
                                  )
                         ) jt
         log errors into spst.err$_kyc_management_customer_block_status_changes('spst.kyc_management_customer_block_status_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_block_status_changes;

procedure load_aml_block_changes (p_message clob)
is

  lc_prc varchar2(50) := 'load_aml_block_changes';

begin

  insert into spst.kyc_management_customer_aml_block_changes ( acc_index, 
                                                               punter_key, 
                                                               punter_id,
                                                               block_id,
                                                               msg_type,
                                                               valid_from,
                                                               valid_to,
                                                               msg_timestamp_utc,
                                                               msg_id,
                                                               created_on )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              block_id,
              msg_type,
              CAST (FROM_TZ (valid_from, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) valid_from,
              CAST (FROM_TZ (valid_to, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) valid_to,
              msg_timestamp_utc,
              msg_id,
              sysdate created_on
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (msg_id             varchar2(255) path '$.id',
                                   msg_timestamp_utc  timestamp(9)  path '$.timestamp',
                                   block_id           number        path '$.blockId',
                                   acc_index          number        path '$.accountIndex',
                                   valid_from         timestamp(9)  path '$.validFrom',
                                   valid_to           timestamp(9)  path '$.validTo',
                                   msg_type           varchar2(100) path '$.type'
                                                                      )
                         ) jt
         log errors into spst.err$_kyc_management_customer_aml_block_changes('spst.kyc_management_customer_aml_block_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_aml_block_changes;

procedure load_communication_changes (p_message clob)
is

  lc_prc varchar2(50) := 'load_communication_changes';

begin

  insert into spst.kyc_management_customer_communication_changes (acc_index, 
                                                                  punter_key, 
                                                                  punter_id,
                                                                  start_communication_at,
                                                                  first_reminder_at,
                                                                  block_at,
                                                                  canceled_at,
                                                                  msg_id,
                                                                  msg_type,
                                                                  msg_published_at_utc,
                                                                  created_on )
       select acc_index,
              get_punter_key(jt.acc_index),
              get_punter_id(jt.acc_index),
              to_date(start_communication_at,'yyyy-mm-dd') start_communication_at,
              to_date(first_reminder_at,'yyyy-mm-dd') first_reminder_at,
              to_date(block_at,'yyyy-mm-dd') block_at,
              CAST (FROM_TZ (canceled_at, 'UTC') AT TIME ZONE 'Europe/Stockholm' AS timestamp(9)) canceled_at,
              msg_id,
              msg_type,
              msg_published_at_utc,
              sysdate created_on
         from dual,
              json_table (
                          p_message,
                          '$'
                          columns (msg_id                 varchar2(255) path '$.id',
                                   msg_type               varchar2(100) path '$.type',
                                   acc_index              number        path '$.accountIndex',
                                   start_communication_at varchar2(10)  path '$.startCommunicationAt',
                                   first_reminder_at      varchar2(10)  path '$.firstReminderAt',
                                   block_at               varchar2(10)  path '$.blockAt', 
                                   canceled_at            timestamp(9)  path '$.canceledAt',                                 
                                   msg_published_at_utc   timestamp(9)  path '$.publishedAt'
                                  )
                         ) jt
         log errors into spst.err$_kyc_management_customer_communication_changes('spst.kyc_management_customer_communication_changes') reject limit unlimited;
                                   
  exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end load_communication_changes;

procedure insert_risk_group_current (p_load_date in date) is
lc_prc                                 varchar2(30) := 'insert_risk_group_current';
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

   insert /*+ APPEND */ into spst.kyc_management_customer_risk_group_current (time_key, 
                                                                                punter_key, 
                                                                                punter_id, 
                                                                                acc_index, 
                                                                                change_valid_from, 
                                                                                change_valid_to, 
                                                                                current_gambling_amount, 
                                                                                risk_group,
                                                                                created_on)
  	   (select to_char(p_load_date,'yyyymmdd'),
               punter_key,
               punter_id,
               acc_index,
               change_valid_from, 
               change_valid_to, 
               current_gambling_amount, 
               risk_group,
               sysdate
       	  from (select punter_key, 
                       punter_id, 
                       acc_index, 
                       change_valid_from, 
                       change_valid_to, 
                       current_gambling_amount, 
                       risk_group,
                       row_number() over (partition by punter_key order by change_valid_from desc, change_valid_to desc nulls first, published_at desc) r
                  from spst.kyc_management_customer_risk_group_changes
                 where change_valid_from_without_time <= p_load_date) x
          where x.r=1);

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
end insert_risk_group_current;

procedure insert_block_status_current (p_load_date in date) is
lc_prc                                 varchar2(30) := 'insert_block_status_current';
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

   insert /*+ APPEND */ into spst.kyc_management_customer_block_status_current (time_key, 
                                                                                punter_key, 
                                                                                punter_id, 
                                                                                acc_index, 
                                                                                questionnaire_date,
                                                                                status_reason,
                                                                                status,
                                                                                status_changed_at)
  	   (select to_char(p_load_date,'yyyymmdd'),
               punter_key,
               punter_id,
               acc_index,
               questionnaire_date,
               status_reason,
               status,
               status_changed_at
       	  from (select punter_key, 
                       punter_id, 
                       acc_index, 
                       questionnaire_date,
                       status_reason,
                       status,
                       status_changed_at,
                       row_number() over (partition by punter_key order by published_at desc) r
                  from spst.kyc_management_customer_block_status_changes
                 where questionnaire_date_wo_time <= p_load_date) x
          where x.r=1);

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
end insert_block_status_current;

procedure insert_aml_block_current (p_load_date in date) is
lc_prc                                 varchar2(30) := 'insert_aml_block_current';
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


   insert /*+ APPEND */ into spst.kyc_management_customer_aml_block_current (time_key
                                                                            ,punter_key
                                                                            ,punter_id
                                                                            ,acc_index
                                                                            ,block_id
                                                                            ,msg_type
                                                                            ,valid_from
                                                                            ,valid_to
                                                                            ,msg_timestamp_utc
                                                                            ,msg_id
                                                                            ,created_on)
  	    select to_char(p_load_date,'yyyymmdd') time_key
              ,punter_key
              ,punter_id
              ,acc_index
              ,block_id
              ,msg_type
              ,valid_from
              ,valid_to
              ,msg_timestamp_utc
              ,msg_id
              ,sysdate
       	  from (select punter_key 
                      ,punter_id 
                      ,acc_index 
                      ,block_id
                      ,msg_type
                      ,valid_from
                      ,valid_to
                      ,msg_timestamp_utc
                      ,msg_id
                      ,row_number() over (partition by punter_key order by msg_timestamp_utc desc) r
                  from spst.kyc_management_customer_aml_block_changes
                 where valid_from <= p_load_date
               ) x
          where x.r=1;

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
end insert_aml_block_current;


function get_last_run_date return date is

  lc_prc varchar2(30) := 'get_last_run_date';
  lv_max_date date := null;
  lv_return_date date := null;

begin

  lv_max_date := null;
  
  --for first load assume that we should run data for yesterday
  select coalesce(max(loaddate),trunc(sysdate-2)) into lv_max_date from dmspst.laddlogg where metaapp = 'KYC_MANAGEMENT';
  
  lv_return_date:=lv_max_date;

  return lv_return_date;

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end get_last_run_date;

procedure insert_message_history (p_message clob, p_message_type varchar2) is
  lc_prc varchar2(30) := 'insert_message_history';
begin

  insert into kyc_management_dequeue_hist (message, message_type, created_date)
       values (p_message, p_message_type, sysdate);

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;

end insert_message_history;

procedure kyc_management_etl
is

  lc_prc varchar2(30) := 'kyc_management_etl';

  dequeue_options dbms_aq.dequeue_options_t;
  message_properties dbms_aq.message_properties_t;

  jms_text_message sys.aq$_jms_text_message;
  text_message clob; 
  system_generated_id_of_the_msg raw(16);

  exc_no_message exception;
  pragma exception_init(exc_no_message, -25228);

  exc_listenerror     exception;
  pragma              exception_init(exc_listenerror,-25254);

  lv_message_type varchar2(100);

begin
  dequeue_options.navigation := dbms_aq.first_message;
  dequeue_options.wait := 0;

  <<inner_loop>>
  loop
    begin

      sys.dbms_aq.dequeue(
                           queue_name          => 'KYC_MANAGEMENT_INBOX_QUEUE',
                           dequeue_options     => dequeue_options,
                           message_properties  => message_properties,
                           payload             => jms_text_message,
                           msgid               => system_generated_id_of_the_msg);

      jms_text_message.get_text(text_message);

      lv_message_type := upper(coalesce(json_value(text_message, '$.type' returning varchar2(255)),'UNKNOWN'));

      case 
         when lv_message_type = 'RISK_GROUP_UPDATED_EVENT' then
            insert_message_history(text_message,lv_message_type);
            load_risk_group_changes(text_message);
         when lv_message_type = 'CUSTOMER_BLOCK_STATUS_CHANGED_EVENT' then
            insert_message_history(text_message,lv_message_type);
            load_block_status_changes(text_message);
         when lv_message_type in ('AML_BLOCK', 'AML_BLOCK_DELETED') then
            insert_message_history(text_message,lv_message_type);
            load_aml_block_changes(text_message);
         when lv_message_type in ('COMMUNICATION_STARTED_EVENT', 'COMMUNICATION_CANCELED_EVENT') then
            insert_message_history(text_message,lv_message_type);
            load_communication_changes(text_message);
         else
            insert_message_history(text_message,lv_message_type);
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

exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end kyc_management_etl;

/*procedure create_files(p_run_date in date) is

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

end create_files;*/

procedure check_error_message(p_table in varchar2)
is

  lc_prc varchar2(30) := 'check_error_message';
  exc_error_found  exception;
  exc_no_table  exception;
  lv_no_of_error_message number;

begin

  case p_table
    when kycriskgroup then
      delete from spst.err$_kyc_management_customer_risk_group_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_kyc_management_customer_risk_group_changes;
     when kycblockstatus then
      delete from spst.err$_kyc_management_customer_block_status_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_kyc_management_customer_block_status_changes;
    when kycamlblockchanges then
      delete from spst.err$_kyc_management_customer_aml_block_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_kyc_management_customer_aml_block_changes;
    when kyccommunicationchanges then
      delete from spst.err$_kyc_management_customer_communication_changes where ora_err_number$=1; --in case we got dublicates delete them and proceed
      select count(*) into lv_no_of_error_message
        from spst.err$_kyc_management_customer_communication_changes;    
    else
      raise exc_no_table;
    end case;

  if lv_no_of_error_message > 0 then
    raise exc_error_found;
  end if;

exception
  when exc_error_found then
    raise_application_error (-20002, 'Errors found in kyc management error table ERR$_' || p_table || '!');
  when exc_no_table then
    raise_application_error (-20003, 'Table name '||p_table|| ' is not supported within prc '||lc_prc||'.');
  when others then
    utilities.log_error(gc_job_name, gc_package, lc_prc);
    raise;
end check_error_message;

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
  l_str := 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD HH24:MI:SS''';
  execute immediate l_str;
  
  execute immediate q'c alter session set TIME_ZONE = 'Europe/Stockholm'c';

  kyc_management_etl;

  commit;

  --Kolla så att inte några fel hamnat i error-tabellen innan jobbet går vidare.
  --Om fel hittas stannar jobbet. Gör då följande:
  --1. Fixa felet och inserta sedan raden manuellt från error-tabellen till aktuell måltabell
  --2. Ta bort raden från error-tabellen
  --3. Starta om jobbet.
  check_error_message(kycriskgroup);
  check_error_message(kycblockstatus);
  check_error_message(kycamlblockchanges);
  check_error_message(kyccommunicationchanges);

  lv_last_run_date:= get_last_run_date;
  lv_end_date:=trunc(sysdate-1);

  lv_current_run_date:=lv_last_run_date+1;

  while lv_current_run_date <=  lv_end_date loop
  
    insert_risk_group_current (lv_current_run_date);
    
    insert_block_status_current (lv_current_run_date);
    
    insert_aml_block_current (lv_current_run_date);
    
    -- insert_communication_current(lv_current_run_date); was replaced with the sql_macro spst.kyc_management_customer_communication_current_fnc(p_date);
    
    --create_files(lv_current_run_date); --om filer skulle behöva skapas till t.ex. Adobe
    
    insert into dmspst.laddlogg (loaddate, klar, metaapp)
         values (lv_current_run_date,'J','KYC_MANAGEMENT');
         
    lv_current_run_date:=lv_current_run_date+1;

  end loop;
  
exception
    when others then
      utilities.log_error(gc_job_name, gc_package, lc_prc);
      raise;
end do;

end kyc_management_job;
