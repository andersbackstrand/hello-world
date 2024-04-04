CREATE OR REPLACE package body SPORTSBOOK.create_files as
/******************************************************************************
   NAME:       create_files
   PURPOSE: 

   REVISIONS:
   Ver        Date        Author               Description
   ---------  ----------  -------------------  ------------------------------------
   1.0        2019-10-01  magnus.pahlen  1. Created this package.
 *****************************************************************************/

    JOB_NAME            constant varchar2(512) := 'create_files_job';
    PACKAGE_NAME        constant varchar2(512) := 'create_files';
    gc_unsettled_date   constant varchar2(512) := '20181001000000';
    gc_loop_param_name  constant varchar2(512) := 'CreateFile';

procedure log_error(p_subprogram_name varchar2)
is
pragma autonomous_transaction;
    
    errcode     varchar2(4000);
    errmsg      varchar2(4000);

procedure write_to_file(p_errcode varchar2, p_errmsg varchar2)
is
pragma autonomous_transaction;

    LARM_LIBRARY      constant varchar2(512) := 'LARM_LIBRARY';
    LARM_FILENAME     constant varchar2(512) := 'LARM_FILNAMN';
    APPEND_TO_FILE    constant varchar2(512) := 'a';
    
    file_handle       utl_file.file_type;
    log_location      varchar2(512);
    file_name         varchar2(512);
    error_message     varchar2(4000);

begin
    log_location := dmspst.stat_generell_dm.hemta_generellparam(LARM_LIBRARY, sysdate);
    file_name    := dmspst.stat_generell_dm.hemta_generellparam(LARM_FILENAME, sysdate);

    error_message := 
        'Job name: '        ||  JOB_NAME            ||
        ', Created on: '    ||  sysdate             ||
        ', Error code: '    ||  p_errcode           ||
        ', Error message: ' ||  p_errmsg;

    file_handle := UTL_FILE.FOPEN(log_location, file_name, APPEND_TO_FILE);
    UTL_FILE.PUT_LINE(file_handle, error_message);
    UTL_FILE.FCLOSE(file_handle);

exception
    when others then
        null;
end write_to_file;

begin
    errcode := to_char(sqlcode);
    errmsg := sqlerrm;

    insert into error_log (
        job_name, package_name, subprogram_name,
        error_code, error_message,
        backtrace, errorstack, callstack,
        created_on, created_by)
    values (
        JOB_NAME, PACKAGE_NAME, p_subprogram_name,
        errcode, errmsg,
        sys.dbms_utility.format_error_backtrace,
        sys.dbms_utility.format_error_stack,
        sys.dbms_utility.format_call_stack,
        sysdate, user);

    commit;    
    write_to_file(errcode, errmsg);

exception
    when others then
        raise;
end log_error;

------------------------------------------------------------------------------------

procedure set_customer_segment_context(p_date in date, p_days_ago in number)
as
l_num number := 0;
begin
   l_num:=set_customer_segment_context_fcn(p_date, p_days_ago);
end set_customer_segment_context;

------------------------------------------------------------------------------------

function set_customer_segment_context_fcn(p_date in date, p_days_ago in number) return number
as
ld_start_date date;
l_num number := 0;
begin
     dbms_session.set_context( 'customer_segment_end_date_ctx', 'p_date', to_char(p_date,'YYYY-mm-dd'));
     ld_start_date := p_date-p_days_ago;
     dbms_session.set_context( 'customer_segment_start_date_ctx', 'ld_start_date', to_char(ld_start_date,'YYYY-mm-dd'));     
     return l_num;
end set_customer_segment_context_fcn;

------------------------------------------------------------------------------------
procedure init_file_creation is
    ld_available_from date;
    ld_available_to date;
    ld_harvested_on date;
    ln_num_of_rows number;
    C_ADD_DAYS constant number := 1;
pragma autonomous_transaction;
begin
   -- 1. Check if we have data to prepare for create files for one day
   select max(available_from),
          max(available_to),
          count(*) 
     into ld_available_from,
          ld_available_to,
          ln_num_of_rows
     from file_status;
   
   if ln_num_of_rows=0 then
      select min(harvested_on), min(harvested_on) 
        into ld_available_to, ld_available_from
        from sportsbook_kambi_stage.dw_status
       where presented_on is not null;
   end if;

     
   select max(harvested_on)+1/24
     into ld_harvested_on
     from sportsbook_kambi_stage.dw_status
    where presented_on is not null
      and (harvested_on+1/24) >= ld_available_to+1;   
    
    -- If we are going to create files for one day
    if trunc(coalesce(ld_harvested_on,ld_available_from)) > trunc(ld_available_from) then
       insert into file_status (available_from, available_to, created_on) 
          values (trunc(ld_available_to), trunc(ld_available_to)+C_ADD_DAYS, sysdate);
    end if;
    commit;

exception
    when others then
        log_error('init_file_creation');
        raise;
end init_file_creation;

------------------------------------------------------------------------------------
function start_file_creation return number is
  ld_available_from date;
  ld_available_to date;
  ld_started_on date;
  ln_count number;
  ln_id number;
pragma autonomous_transaction;
begin

   select min(available_from), 
          min(available_to), 
          sysdate,
          count(*) 
     into ld_available_from, 
          ld_available_to, 
          ld_started_on,
          ln_count
     from file_status 
    where started_on is null;
     
   if ln_count > 0 then
       -- Aggregate customer CRM Base before file creation
       customer_activity_job.do(trunc(ld_available_from));
       update file_status 
          set started_on = ld_started_on
        where available_from = ld_available_from 
          and available_to = ld_available_to
          and started_on is null
        returning id into ln_id;
       commit;
       return ln_id;
   else 
       return null;
   end if;
   
exception
    when others then
        log_error('start_file_creation');
        raise;
end start_file_creation;

------------------------------------------------------------------------------------
procedure end_file_creation(p_id number) is
pragma autonomous_transaction;
begin
   update file_status set finished_on = sysdate where id=p_id;
   commit;
exception
    when others then
        log_error('end_file_creation');
        raise;
end end_file_creation;

------------------------------------------------------------------------------------
procedure error_file_creation(p_id number) is
pragma autonomous_transaction;
begin
   update file_status 
      set prev_started_on = started_on, 
          started_on      = null, 
          updated_on      = sysdate 
    where id = p_id;
   commit;
exception
    when others then
        log_error('error_file_creation');
        raise;
end error_file_creation;

------------------------------------------------------------------------------------
procedure file_creation(p_id number) is
  lv_available_from                  varchar2(100);
  lv_available_from_90_days_ago      varchar2(100);  
  lv_available_to                    varchar2(100);
  lv_parametervarde                  varchar2(500);
  lv_file_name_date                  varchar2(100);
  C_GROUP                            constant number        := 8;
  C_SPORTSBOOK_CF_SHORT              constant varchar2(200) := 'SPORTSBOOK_CF_SHORT';
  C_SHORT                            constant number        := 91;
  C_SPORTSBOOK_CF_LONG               constant varchar2(200) := 'SPORTSBOOK_CF_LONG';
  C_LONG                             constant number        := 364;
  C_SPORTSBOOK                       constant varchar2(200) := 'SPORTSBOOK';  
  C_SPORTSBOOK_PARAM1                constant varchar2(200) := 'SPORTSBOOK_PARAM1'; -- ADOBE CRM file creation 
  C_SPORTSBOOK_PARAM2                constant varchar2(200) := 'SPORTSBOOK_PARAM2';
  C_SPORTSBOOK_PARAM3                constant varchar2(200) := 'SPORTSBOOK_PARAM3'; 
  C_SPORTSBOOK_PARAM4                constant varchar2(200) := 'SPORTSBOOK_PARAM4';
  C_SPORTSBOOK_PARAM5                constant varchar2(200) := 'SPORTSBOOK_PARAM5';  
  C_AC_SPORTSBOOK                    constant varchar2(200) := 'AC_SPORTSBOOK';
  C_AC_SPORTSBOOK_PARAM1             constant varchar2(200) := 'AC_SPORTSBOOK_PARAM1';
  C_DATE_FORMAT                      constant varchar2(200) := 'YYYYMMDDHH24MISS';
  C_DATE_FORMAT_FILE_NAME            constant varchar2(200) := 'YYYYMMDD';
  C_DATE_DD_FORMAT                   constant varchar2(200) := 'DD';
  
  l_klarfilnamn varchar2(200) := 'laddatum_klarfil_sport_'||to_char(sysdate,'YYYYMMDDHH24MISS')||'.csv';
  l_rows        number;
  l_fraga       varchar2(2000);
  l_separator   varchar2(1) := '|';
  l_katalog     varchar2(30) := 'QLIK';   
  
begin
   select to_char(available_from,C_DATE_FORMAT),
          to_char(available_from-90,C_DATE_FORMAT), -- only for bets 
          to_char(available_from,C_DATE_FORMAT_FILE_NAME),
          to_char(available_to,C_DATE_FORMAT) 
     into lv_available_from,
          lv_available_from_90_days_ago,
          lv_file_name_date,
          lv_available_to
     from file_status 
    where id=p_id;
    
    for r_app in (select applikation from dmspst.filgen_applikation_grupp where grupp=C_GROUP order by gruppordning) loop
        if r_app.applikation=C_SPORTSBOOK_CF_SHORT then
           -- set context for frequency views 
           set_customer_segment_context(to_date(lv_available_to,C_DATE_FORMAT),C_SHORT);
           lv_parametervarde := null;
        end if;        
        if r_app.applikation=C_SPORTSBOOK_CF_LONG then
           -- set context for frequency views 
           set_customer_segment_context(to_date(lv_available_to,C_DATE_FORMAT),C_LONG);
           lv_parametervarde := null;
        end if;        
        if r_app.applikation=C_SPORTSBOOK or r_app.applikation=C_AC_SPORTSBOOK then
           lv_parametervarde := null;
        end if;
        if r_app.applikation=C_SPORTSBOOK_PARAM1 then
--           lv_file_name_date := C_CRM_FILE_NAME_PREFIX||lv_file_name_date;
           lv_parametervarde := 'to_date('||dbms_assert.enquote_literal(lv_available_from)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')';        
        end if;
        if r_app.applikation=C_SPORTSBOOK_PARAM2 then
           lv_parametervarde := 'to_date('||dbms_assert.enquote_literal(lv_available_from_90_days_ago)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')'
                                ||' and settled_date < '||'to_date('||dbms_assert.enquote_literal(lv_available_to)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')';          
        end if;        
        if r_app.applikation=C_SPORTSBOOK_PARAM3 then
           lv_parametervarde := 'to_date('||dbms_assert.enquote_literal(gc_unsettled_date)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')'
                                ||' and created_date < '||'to_date('||dbms_assert.enquote_literal(lv_available_to)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')';  
        end if;

        if r_app.applikation=C_SPORTSBOOK_PARAM4 then
           lv_parametervarde := 'to_date('||dbms_assert.enquote_literal(lv_available_from)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')'
                                ||' and bet_offer_settled_date < '||'to_date('||dbms_assert.enquote_literal(lv_available_to)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')';          
        end if;  
        
        if r_app.applikation=C_SPORTSBOOK_PARAM5 then
           lv_parametervarde := 'to_date('||dbms_assert.enquote_literal(lv_available_from)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')'
                                ||' and bet_offer_opens_date < '||'to_date('||dbms_assert.enquote_literal(lv_available_to)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')';          
        end if;    

        if r_app.applikation=C_AC_SPORTSBOOK_PARAM1 then
           lv_parametervarde := 'to_date('||dbms_assert.enquote_literal(lv_available_from)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||')'
                                ||' and created_date_day_of_month = '||'to_number(to_char(to_date('||dbms_assert.enquote_literal(lv_available_from)||','||dbms_assert.enquote_literal(C_DATE_FORMAT)||'),'||dbms_assert.enquote_literal(C_DATE_DD_FORMAT)||'))';
        end if;         
        
           DMSPST.skapa_filer.skapa_filer
                                   (
                                    pin_applikation => r_app.applikation,
                                    laddatum        => lv_file_name_date,
                                    parametervarde  => lv_parametervarde
                                   );
           lv_parametervarde := null;
    end loop;
    
    l_fraga := 'SELECT '||sys.dbms_assert.enquote_literal('laddatum') ||' FROM DUAL';
    l_fraga := l_fraga||' union all ';
    l_fraga := l_fraga||' SELECT '||sys.dbms_assert.enquote_literal(lv_file_name_date)||' FROM DUAL';
    l_rows :=   dmspst.skapa_filer.dump_usv( p_query     => l_fraga,
                                             p_separator => l_separator,
                                             p_dir       => l_katalog,
                                             p_filename  => l_klarfilnamn,
                                             p_mode      => 'w'); 
  
  exception    
    when others then
        log_error('file_creation');
        error_file_creation(p_id); 
        raise;
end file_creation;

------------------------------------------------------------------------------------

procedure do as
lc_prc                         varchar2(30) := 'do'; 
ln_id number;
lv_loop varchar2(1) := 'N';
ld_date_to date;
ld_available_to date;
begin

    begin
        select v_value, 
               d_value
          into lv_loop,
               ld_date_to
          from sportsbook_kambi_stage.parms
         where parameter=gc_loop_param_name;
    exception
        when no_data_found then
            null; --If the parameter is not in the table execute the creation file once, no errors
    end;

    if lv_loop='Y' and ld_date_to is not null then
       select max(available_to) into ld_available_to from file_status; 
    end if;

    if ld_available_to is null or ld_date_to is null then
       ld_date_to:=to_date('19010101','yyyymmdd');
       ld_available_to:=to_date('19010101','yyyymmdd');       
    end if;
    
    while ld_available_to<=ld_date_to loop
        init_file_creation;
        ln_id := start_file_creation;
        if ln_id is not null then
           file_creation(ln_id);
           end_file_creation(ln_id);
        end if;
        ld_available_to:=ld_available_to+1;
    end loop;
    
exception
   when others then
        log_error('do');
        raise;
end do;

end create_files;
