CREATE OR REPLACE PACKAGE BODY SPST.customer_activity_job as
/******************************************************************************
   NAME:       customer_activity_job --TEST
   PURPOSE:    TEST 3.

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2017/05/09      fredrik.reutersward       1. Created this package.
   1.0        2017/05/11  magnus.pahlen     2. created insert_one_date_time_day,
              2017/05/12                       created get_time_day_id
 *****************************************************************************/

    JOB_NAME
        constant varchar2(512) := 'customer_activity_job';
										
    HORSE_RACING_ID   constant pls_integer := 1001;
    VIRTUAL_RACING_ID constant pls_integer := 1002;

    HORSE_RACING   constant varchar2(512) := 'HORSE RACING';
    VIRTUAL_RACING constant varchar2(512) := 'VIRTUAL RACING';

    TROT_ID   constant pls_integer := 1001;
    GALLOP_ID constant pls_integer := 1002;

    TROT    constant varchar2(512) := 'TROT';
    GALLOP  constant varchar2(512) := 'GALLOP';

    ROLLING_LONG_WEEK         constant pls_integer := 52;
    ROLLING_SHORT_WEEK        constant pls_integer := 13;

    COMMIT
        constant varchar2(512)  := 'commit';

    YYYYMMDD        constant varchar2(512)  := 'yyyymmdd';	
    DDMONYY         constant varchar2(512)  := 'dd-mon-yy';
    YYYYMMDDHH24    constant varchar2(512)  := 'yyyymmddhh24';

    ZERO            constant pls_integer := 0;
    ONE             constant pls_integer := 1;
    TWO             constant pls_integer := 2;
    THREE           constant pls_integer := 3;
    SEVEN           constant pls_integer := 7;
    EIGHT           constant pls_integer := 8;
    NINE            constant pls_integer := 9;
    TEN             constant pls_integer := 10;

    TYPE_OF_ACCOUNT     constant varchar2(512) := 'PERSON';
------------------------------------------------------------------------------
------------------------------------------------------------------------------
function drop_partition_for_date(p_date date) return boolean
is
    ALTER_TABLE_DROP_PARTITION
      constant varchar2(512)  :=
      'alter table ca_time_day drop partition for (to_date(''';

    YYYY_MM_DD      constant varchar2(512)  := 'yyyy-mm-dd';

    partition_does_not_exist    exception;
    pragma exception_init(partition_does_not_exist, -2149 );

    MODULE_NAME            constant varchar2(30) := 'drop_partition_for_date';
    JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin
  execute immediate
    ALTER_TABLE_DROP_PARTITION                ||
      to_char(p_date,YYYY_MM_DD)              ||
      ''','                                   ||
      dbms_assert.enquote_literal(YYYY_MM_DD) ||
      ')) UPDATE INDEXES';

  return true;

exception
   when partition_does_not_exist then
      return false;
   when others then
      dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
      raise;
end drop_partition_for_date;
------------------------------------------------------------------------------
------------------------------------------------------------------------------
procedure rebuild_index
is
  
  type attribute_t is record(
    attribute_text    varchar2(256));

  type index_array_t is table of attribute_t
    index by pls_integer;

  index_array      index_array_t;

  TABLE_CA_TIME_DAY
    constant varchar2(512)  := 'CA_TIME_DAY';
  ALTER_INDEX
    constant varchar2(512)  := 'alter index ';
  REBUILD
    constant varchar2(512)  := ' rebuild';

    MODULE_NAME            constant varchar2(30) := 'rebuild_index';
    JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

   ld_start_time date;
   ln_batch_log_id         batch_log.id%type;
begin

    ld_start_time:=sysdate;
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  select index_name bulk collect into index_array
  from user_indexes where table_name = TABLE_CA_TIME_DAY;

  if not index_array.exists(ONE) then return; end if;

  for i in index_array.first .. index_array.last
  loop
    execute immediate ALTER_INDEX || index_array(i).attribute_text || REBUILD;
  end loop;

    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => 1,
                             pio_id => ln_batch_log_id);

exception
   when others then
      dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
      raise;
end rebuild_index;
------------------------------------------------------------------------------
------------------------------------------------------------------------------
procedure set_slowly_changing_dimensions
is
  NO_DATA_FOUND_ID  constant pls_integer := -1;
  NO_DATA_FOUND     constant varchar2(512) := 'NO DATA FOUND';
  MODULE_NAME            constant varchar2(30) := 'set_slowly_changing_dimensions';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

  row_count   pls_integer;
-----------------------------------------
  procedure game_type
  is
  begin
   select count(*) into row_count from ca_game_type
   where id = HORSE_RACING_ID and game_type = HORSE_RACING;

   if row_count = ZERO then
      insert into ca_game_type(id, game_type)
      values(HORSE_RACING_ID, HORSE_RACING);
   end if;

   select count(*) into row_count from ca_game_type
   where id = VIRTUAL_RACING_ID and game_type = VIRTUAL_RACING;

   if row_count = ZERO then
      insert into ca_game_type (id, game_type)
      values(VIRTUAL_RACING_ID, VIRTUAL_RACING);
   end if;

   select count(*) into row_count from ca_game_type
   where id = NO_DATA_FOUND_ID and game_type = NO_DATA_FOUND;

   if row_count = ZERO then
      insert into ca_game_type (id, game_type)
      values(NO_DATA_FOUND_ID, NO_DATA_FOUND);
   end if;

  exception
    when others then
      raise;
  end game_type;
----------------------------------------------
  procedure race_type
  is
  begin
   select count(*) into row_count from ca_race_type
   where id = TROT_ID and race_type = TROT;

   if row_count = ZERO then
      insert into ca_race_type (id, race_type)
      values(TROT_ID, TROT);
   end if;

   select count(*) into row_count from ca_race_type
   where id = GALLOP_ID and race_type = GALLOP;

   if row_count = ZERO then
      insert into ca_race_type (id, race_type)
      values(GALLOP_ID, GALLOP);
   end if;

   select count(*) into row_count from ca_race_type
   where id = NO_DATA_FOUND_ID and race_type = NO_DATA_FOUND;

   if row_count = ZERO then
      insert into ca_race_type (id, race_type)
      values(NO_DATA_FOUND_ID, NO_DATA_FOUND);
   end if;

  exception
    when others then
      raise;
  end race_type;
-----------------------------------------------
  procedure channel_type
  is
      type attribute_t is record(
      attribute_id      pls_integer,
      attribute_text    varchar2(256));

      type attribute_array_t is table of attribute_t
        index by pls_integer;

      attributes      attribute_array_t;

  begin
      select count(*) into row_count from ca_channel_type
      where id = NO_DATA_FOUND_ID and channel_type = NO_DATA_FOUND;

      if row_count = ZERO then
        insert into ca_channel_type (id, channel_type)
        values(NO_DATA_FOUND_ID, NO_DATA_FOUND);
      end if;

      select id, can_type
          bulk collect into attributes
      from channeltype
      where can_type not in
      (select channel_type from ca_channel_type);

      if attributes.exists(ONE) then null; else return; end if;

      forall i in attributes.first .. attributes.last
            insert into ca_channel_type(id, channel_type) values(
                  ca_channel_type_seq.nextval,
                  attributes(i).attribute_text);
  exception
    when others then
      raise;
  end channel_type;
----------------------------------------------
  procedure bet_method_group
  is

    MODULE_NAME            constant varchar2(30) := 'bet_method_group';
    JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

      type attribute_t is record(
      attribute_id      pls_integer,
      attribute_text    varchar2(256));

      type attribute_array_t is table of attribute_t
        index by pls_integer;

      attributes      attribute_array_t;

      function is_row (p_id pls_integer, p_text varchar2) return boolean
      is
        row_count   pls_integer;
      begin
        select count(*) into row_count
        from   ca_bet_method_group
        where  orig_id = p_id and bet_method_group = p_text;

        return (row_count != ZERO);

      end is_row;

  begin
      select count(*) into row_count from ca_bet_method_group
      where id = NO_DATA_FOUND_ID and bet_method_group = NO_DATA_FOUND;

      if row_count = ZERO then
        insert into ca_bet_method_group (id, orig_id, bet_method_group)
        values(NO_DATA_FOUND_ID, NO_DATA_FOUND_ID, NO_DATA_FOUND);
      end if;

      select distinct betmethod_group_id, betmethod_group
          bulk collect into attributes
      from betmethod
      order by betmethod_group_id;

      if not attributes.exists(ONE) then return; end if;

      for i in attributes.first .. attributes.last
      loop
        if not is_row(attributes(i).attribute_id, attributes(i).attribute_text) then
            insert into ca_bet_method_group(id, orig_id, bet_method_group) values
            (ca_bet_method_group_seq.nextval,
            attributes(i).attribute_id,
            attributes(i).attribute_text);
        end if;
      end loop;

  exception
    when others then
      raise;
  end bet_method_group;
--------------------------------------------
  procedure program_concept
  is

    PC_ID_1001     constant  pls_integer := 1001;
    PC_ID_1002     constant  pls_integer := 1002;
    PC_ID_1003     constant  pls_integer := 1003;
    PC_ID_1004     constant  pls_integer := 1004;
    PC_ID_1005     constant  pls_integer := 1005;
    PC_ID_1006     constant  pls_integer := 1006;
    PC_ID_1007     constant  pls_integer := 1007;
    PC_ID_1008     constant  pls_integer := 1008;
    PC_ID_1009     constant  pls_integer := 1009;
    PC_ID_1010     constant  pls_integer := 1010;
    PC_ID_1011     constant  pls_integer := 1011;
    PC_ID_1012     constant  pls_integer := 1012;

    PC_ORIG_ID_1001     constant  pls_integer := 1;
    PC_ORIG_ID_1002     constant  pls_integer := 3;
    PC_ORIG_ID_1003     constant  pls_integer := 4;
    PC_ORIG_ID_1004     constant  pls_integer := 6;
    PC_ORIG_ID_1005     constant  pls_integer := 7;
    PC_ORIG_ID_1006     constant  pls_integer := 8;
    PC_ORIG_ID_1007     constant  pls_integer := 9;
    PC_ORIG_ID_1008     constant  pls_integer := 42;
    PC_ORIG_ID_1009     constant  pls_integer := 43;
    PC_ORIG_ID_1010     constant  pls_integer := 46;
    PC_ORIG_ID_1011     constant  pls_integer := 47;
    PC_ORIG_ID_1012     constant  pls_integer := 53;

    PC_CONCEPT_1001     constant  varchar2(512) := 'Lunch-dagar';
    PC_CONCEPT_1002     constant  varchar2(512) := 'Saxade-dagar';
    PC_CONCEPT_1003     constant  varchar2(512) := 'V64-onsdag';
    PC_CONCEPT_1004     constant  varchar2(512) := 'V65-söndag';
    PC_CONCEPT_1005     constant  varchar2(512) := 'V65-vardag';
    PC_CONCEPT_1006     constant  varchar2(512) := 'V75-dagar';
    PC_CONCEPT_1007     constant  varchar2(512) := 'Övriga dagar';
    PC_CONCEPT_1008     constant  varchar2(512) := 'Extra V75';
    PC_CONCEPT_1009     constant  varchar2(512) := 'Inställd';
    PC_CONCEPT_1010     constant  varchar2(512) := 'V86-onsdag';
    PC_CONCEPT_1011     constant  varchar2(512) := 'V64-söndag';
    PC_CONCEPT_1012     constant  varchar2(512) := 'VR';

  begin
      select count(*) into row_count from ca_program_concept
      where id = NO_DATA_FOUND_ID and concept = NO_DATA_FOUND;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(NO_DATA_FOUND_ID, NO_DATA_FOUND_ID, NO_DATA_FOUND);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1001 and orig_id = PC_ORIG_ID_1001 and
        concept = PC_CONCEPT_1001;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1001, PC_ORIG_ID_1001, PC_CONCEPT_1001);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1002 and orig_id = PC_ORIG_ID_1002 and
        concept = PC_CONCEPT_1002;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1002, PC_ORIG_ID_1002, PC_CONCEPT_1002);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1003 and orig_id = PC_ORIG_ID_1003 and
        concept = PC_CONCEPT_1003;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1003, PC_ORIG_ID_1003, PC_CONCEPT_1003);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1004 and orig_id = PC_ORIG_ID_1004 and
        concept = PC_CONCEPT_1004;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1004, PC_ORIG_ID_1004, PC_CONCEPT_1004);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1005 and orig_id = PC_ORIG_ID_1005 and
        concept = PC_CONCEPT_1005;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1005, PC_ORIG_ID_1005, PC_CONCEPT_1005);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1006 and orig_id = PC_ORIG_ID_1006 and
        concept = PC_CONCEPT_1006;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1006, PC_ORIG_ID_1006, PC_CONCEPT_1006);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1007 and orig_id = PC_ORIG_ID_1007 and
        concept = PC_CONCEPT_1007;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1007, PC_ORIG_ID_1007, PC_CONCEPT_1007);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1008 and orig_id = PC_ORIG_ID_1008 and
        concept = PC_CONCEPT_1008;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1008, PC_ORIG_ID_1008, PC_CONCEPT_1008);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1009 and orig_id = PC_ORIG_ID_1009 and
        concept = PC_CONCEPT_1009;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1009, PC_ORIG_ID_1009, PC_CONCEPT_1009);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1010 and orig_id = PC_ORIG_ID_1010 and
        concept = PC_CONCEPT_1010;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1010, PC_ORIG_ID_1010, PC_CONCEPT_1010);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1011 and orig_id = PC_ORIG_ID_1011 and
        concept = PC_CONCEPT_1011;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1011, PC_ORIG_ID_1011, PC_CONCEPT_1011);
      end if;

      select count(*) into row_count from ca_program_concept
      where id = PC_ID_1012 and orig_id = PC_ORIG_ID_1012 and
        concept = PC_CONCEPT_1012;
      if row_count = ZERO then
        insert into ca_program_concept (id, orig_id, concept)
        values(PC_ID_1012, PC_ORIG_ID_1012, PC_CONCEPT_1012);
      end if;

  exception
    when others then
      raise;
  end program_concept;
------------------------------------------------

begin

  game_type;
  race_type;
  channel_type;
  bet_method_group;
  program_concept;

exception
   when others then
      dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
      raise;
end set_slowly_changing_dimensions;
------------------------------------------------------------------------------
procedure set_meta_data_repository
is
  MODULE_NAME            constant varchar2(30) := 'set_meta_data_repository';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

  row_count   pls_integer;

  procedure set_cluster
  is

    CLUSTER_ID_1001     constant  pls_integer := 1001;
    CLUSTER_ID_1002     constant  pls_integer := 1002;
    CLUSTER_ID_1003     constant  pls_integer := 1003;
    CLUSTER_ID_1004     constant  pls_integer := 1004;
    CLUSTER_ID_1005     constant  pls_integer := 1005;
    CLUSTER_ID_1006     constant  pls_integer := 1006;
    CLUSTER_ID_1007     constant  pls_integer := 1007;
    CLUSTER_ID_1008     constant  pls_integer := 1008;
    CLUSTER_ID_1009     constant  pls_integer := 1009;
    CLUSTER_ID_1010     constant  pls_integer := 1010;
    CLUSTER_ID_1011     constant  pls_integer := 1011;
    CLUSTER_ID_1012     constant  pls_integer := 1012;

    CLUSTER_NAME_1001   constant varchar2(512) := 'V75-Lördags-spelaren';
    CLUSTER_NAME_1002   constant varchar2(512) := 'V75-spelaren';
    CLUSTER_NAME_1003   constant varchar2(512) := 'V75-V86-spelaren';
    CLUSTER_NAME_1004   constant varchar2(512) := 'Specialisten';
    CLUSTER_NAME_1005   constant varchar2(512) := 'Endast Tillsammans-spelare';
    CLUSTER_NAME_1006   constant varchar2(512) := 'Endast Galopp-spelare';
    CLUSTER_NAME_1007   constant varchar2(512) := 'Endast VR-spelare';
    CLUSTER_NAME_1008   constant varchar2(512) := 'Inget kluster';
    CLUSTER_NAME_1009   constant varchar2(512) := 'Endast Tillsammans- och Galopp-spelare';
    CLUSTER_NAME_1010   constant varchar2(512) := 'Endast Tillsammans- och VR-spelare';
    CLUSTER_NAME_1011   constant varchar2(512) := 'Endast Galopp- och VR-spelare';
    CLUSTER_NAME_1012   constant varchar2(512) := 'Endast Tillsammans-, Galopp- och VR-spelare';

  begin

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1001 and cluster_name = CLUSTER_NAME_1001;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1001, CLUSTER_NAME_1001);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1002 and cluster_name = CLUSTER_NAME_1002;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1002, CLUSTER_NAME_1002);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1003 and cluster_name = CLUSTER_NAME_1003;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1003, CLUSTER_NAME_1003);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1004 and cluster_name = CLUSTER_NAME_1004;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1004, CLUSTER_NAME_1004);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1005 and cluster_name = CLUSTER_NAME_1005;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1005, CLUSTER_NAME_1005);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1006 and cluster_name = CLUSTER_NAME_1006;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1006, CLUSTER_NAME_1006);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1007 and cluster_name = CLUSTER_NAME_1007;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1007, CLUSTER_NAME_1007);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1008 and cluster_name = CLUSTER_NAME_1008;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1008, CLUSTER_NAME_1008);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1009 and cluster_name = CLUSTER_NAME_1009;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1009, CLUSTER_NAME_1009);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1010 and cluster_name = CLUSTER_NAME_1010;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1010, CLUSTER_NAME_1010);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1011 and cluster_name = CLUSTER_NAME_1011;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1011, CLUSTER_NAME_1011);
   end if;

   select count(*) into row_count from ca_cluster
   where id = CLUSTER_ID_1012 and cluster_name = CLUSTER_NAME_1012;
   if row_count = ZERO then
      insert into ca_cluster(id, cluster_name)
      values(CLUSTER_ID_1012, CLUSTER_NAME_1012);
   end if;

  exception
    when others then
      raise;
  end set_cluster;

  procedure set_priority
  is

    PRIORITY_ID_1001     constant  pls_integer := 1001;
    PRIORITY_ID_1002     constant  pls_integer := 1002;
    PRIORITY_ID_1003     constant  pls_integer := 1003;

    PRIORITY_ITEM_1001     constant  pls_integer := 1;
    PRIORITY_ITEM_1002     constant  pls_integer := 2;
    PRIORITY_ITEM_1003     constant  pls_integer := 3;

    PERCENTAGE_ITEM_1001     constant  number := 0.8;
    PERCENTAGE_ITEM_1002     constant  number := 0.8;
    PERCENTAGE_ITEM_1003     constant  number := 0.8;

    NOGD_ITEM_1001     constant  pls_integer := 10;
    NOGD_ITEM_1002     constant  pls_integer := 10;
    NOGD_ITEM_1003     constant  pls_integer := 10;

  begin
   select count(*) into row_count from ca_priority
   where id = PRIORITY_ID_1001 and priority = PRIORITY_ITEM_1001 and
   percentage = PERCENTAGE_ITEM_1001 and num_of_game_days = NOGD_ITEM_1001;
   if row_count = ZERO then
      insert into ca_priority (id, priority, percentage, num_of_game_days)
      values(PRIORITY_ID_1001, PRIORITY_ITEM_1001, PERCENTAGE_ITEM_1001, NOGD_ITEM_1001);
   end if;

   select count(*) into row_count from ca_priority
   where id = PRIORITY_ID_1002 and priority = PRIORITY_ITEM_1002 and
   percentage = PERCENTAGE_ITEM_1002 and num_of_game_days = NOGD_ITEM_1002;
   if row_count = ZERO then
      insert into ca_priority (id, priority, percentage, num_of_game_days)
      values(PRIORITY_ID_1002, PRIORITY_ITEM_1002, PERCENTAGE_ITEM_1002, NOGD_ITEM_1002);
   end if;

   select count(*) into row_count from ca_priority
   where id = PRIORITY_ID_1003 and priority = PRIORITY_ITEM_1003 and
   percentage = PERCENTAGE_ITEM_1003 and num_of_game_days = NOGD_ITEM_1003;
   if row_count = ZERO then
      insert into ca_priority (id, priority, percentage, num_of_game_days)
      values(PRIORITY_ID_1003, PRIORITY_ITEM_1003, PERCENTAGE_ITEM_1003, NOGD_ITEM_1003);
   end if;

  exception
    when others then
      raise;
  end set_priority;

  procedure set_cluster_concept
  is

    CC_ID_1001     constant  pls_integer := 1001;
    CC_ID_1002     constant  pls_integer := 1002;
    CC_ID_1003     constant  pls_integer := 1003;
    CC_ID_1004     constant  pls_integer := 1004;
    CC_ID_1006     constant  pls_integer := 1006;
    CC_ID_1007     constant  pls_integer := 1007;

    CLUSTER_ID_1001     constant  pls_integer := 1001;
    CLUSTER_ID_1002     constant  pls_integer := 1002;
    CLUSTER_ID_1003     constant  pls_integer := 1003;

    PC_ID_1006     constant  pls_integer := 1006;
    PC_ID_1008     constant  pls_integer := 1008;
    PC_ID_1010     constant  pls_integer := 1010;

    PRIORITY_ID_1001     constant  pls_integer := 1001;
    PRIORITY_ID_1002     constant  pls_integer := 1002;
    PRIORITY_ID_1003     constant  pls_integer := 1003;

  begin
   select count(*) into row_count from ca_cluster_concept
   where id = CC_ID_1001 and ca_cluster_id = CLUSTER_ID_1001 and
   ca_program_concept_id = PC_ID_1006 and ca_priority_id = PRIORITY_ID_1001;
   if row_count = ZERO then
      insert into ca_cluster_concept (id, ca_cluster_id, ca_program_concept_id, ca_priority_id)
      values(CC_ID_1001, CLUSTER_ID_1001, PC_ID_1006, PRIORITY_ID_1001);
   end if;

   select count(*) into row_count from ca_cluster_concept
   where id = CC_ID_1002 and ca_cluster_id = CLUSTER_ID_1002 and
   ca_program_concept_id = PC_ID_1006 and ca_priority_id = PRIORITY_ID_1002;
   if row_count = ZERO then
      insert into ca_cluster_concept (id, ca_cluster_id, ca_program_concept_id, ca_priority_id)
      values(CC_ID_1002, CLUSTER_ID_1002, PC_ID_1006, PRIORITY_ID_1002);
   end if;

   select count(*) into row_count from ca_cluster_concept
   where id = CC_ID_1003 and ca_cluster_id = CLUSTER_ID_1003 and
   ca_program_concept_id = PC_ID_1006 and ca_priority_id = PRIORITY_ID_1003;
   if row_count = ZERO then
      insert into ca_cluster_concept (id, ca_cluster_id, ca_program_concept_id, ca_priority_id)
      values(CC_ID_1003, CLUSTER_ID_1003, PC_ID_1006, PRIORITY_ID_1003);
   end if;

   select count(*) into row_count from ca_cluster_concept
   where id = CC_ID_1004 and ca_cluster_id = CLUSTER_ID_1003 and
   ca_program_concept_id = PC_ID_1010 and ca_priority_id = PRIORITY_ID_1003;
   if row_count = ZERO then
      insert into ca_cluster_concept (id, ca_cluster_id, ca_program_concept_id, ca_priority_id)
      values(CC_ID_1004, CLUSTER_ID_1003, PC_ID_1010, PRIORITY_ID_1003);
   end if;

   select count(*) into row_count from ca_cluster_concept
   where id = CC_ID_1006 and ca_cluster_id = CLUSTER_ID_1002 and
   ca_program_concept_id = PC_ID_1008 and ca_priority_id = PRIORITY_ID_1002;
   if row_count = ZERO then
      insert into ca_cluster_concept (id, ca_cluster_id, ca_program_concept_id, ca_priority_id)
      values(CC_ID_1006, CLUSTER_ID_1002, PC_ID_1008, PRIORITY_ID_1002);
   end if;

   select count(*) into row_count from ca_cluster_concept
   where id = CC_ID_1007 and ca_cluster_id = CLUSTER_ID_1003 and
   ca_program_concept_id = PC_ID_1008 and ca_priority_id = PRIORITY_ID_1003;
   if row_count = ZERO then
      insert into ca_cluster_concept (id, ca_cluster_id, ca_program_concept_id, ca_priority_id)
      values(CC_ID_1007, CLUSTER_ID_1003, PC_ID_1008, PRIORITY_ID_1003);
   end if;

  exception
    when others then
      raise;
  end set_cluster_concept;

  procedure set_frequency
  is

    CF_ID_1001     constant  pls_integer := 1001;
    CF_ID_1002     constant  pls_integer := 1002;
    CF_ID_1003     constant  pls_integer := 1003;

    FREQUENCY_NAME_1001   constant varchar2(512) := 'Låg';
    FREQUENCY_NAME_1002   constant varchar2(512) := 'Hög';
    FREQUENCY_NAME_1003   constant varchar2(512) := 'Pro';

  begin
   select count(*) into row_count from ca_frequency
   where id = CF_ID_1001 and frequency_name = FREQUENCY_NAME_1001;
   if row_count = ZERO then
      insert into ca_frequency (id, frequency_name)
      values(CF_ID_1001, FREQUENCY_NAME_1001);
   end if;

   select count(*) into row_count from ca_frequency
   where id = CF_ID_1002 and frequency_name = FREQUENCY_NAME_1002;
   if row_count = ZERO then
      insert into ca_frequency (id, frequency_name)
      values(CF_ID_1002, FREQUENCY_NAME_1002);
   end if;

   select count(*) into row_count from ca_frequency
   where id = CF_ID_1003 and frequency_name = FREQUENCY_NAME_1003;
   if row_count = ZERO then
      insert into ca_frequency (id, frequency_name)
      values(CF_ID_1003, FREQUENCY_NAME_1003);
   end if;

  exception
    when others then
      raise;
  end set_frequency;

  procedure set_cluster_interval
  is

    CI_ID_1001     constant  pls_integer := 1001;
    CI_ID_1002     constant  pls_integer := 1002;
    CI_ID_1003     constant  pls_integer := 1003;
    CI_ID_1004     constant  pls_integer := 1004;
    CI_ID_1005     constant  pls_integer := 1005;
    CI_ID_1006     constant  pls_integer := 1006;
    CI_ID_1007     constant  pls_integer := 1007;
    CI_ID_1008     constant  pls_integer := 1008;
    CI_ID_1009     constant  pls_integer := 1009;

    CLUSTER_ID_1001     constant  pls_integer := 1001;
    CLUSTER_ID_1002     constant  pls_integer := 1002;
    CLUSTER_ID_1003     constant  pls_integer := 1003;
    CLUSTER_ID_1004     constant  pls_integer := 1004;

    FREQUENCY_ID_1001     constant  pls_integer := 1001;
    FREQUENCY_ID_1002     constant  pls_integer := 1002;
    FREQUENCY_ID_1003     constant  pls_integer := 1003;

    MIN_VAL_1001     constant  pls_integer := 0;
    MIN_VAL_1002     constant  pls_integer := 0;
    MIN_VAL_1003     constant  pls_integer := 0;
    MIN_VAL_1004     constant  pls_integer := 0;
    MIN_VAL_1005     constant  pls_integer := 6;
    MIN_VAL_1006     constant  pls_integer := 11;
    MIN_VAL_1007     constant  pls_integer := 17;
    MIN_VAL_1008     constant  pls_integer := 20;
    MIN_VAL_1009     constant  pls_integer := 45;

    MAX_VAL_1001     constant  pls_integer := 5;
    MAX_VAL_1002     constant  pls_integer := 10;
    MAX_VAL_1003     constant  pls_integer := 16;
    MAX_VAL_1004     constant  pls_integer := 19;
    MAX_VAL_1005     constant  pls_integer := 9999;
    MAX_VAL_1006     constant  pls_integer := 9999;
    MAX_VAL_1007     constant  pls_integer := 9999;
    MAX_VAL_1008     constant  pls_integer := 44;
    MAX_VAL_1009     constant  pls_integer := 9999;

  begin
   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1001 and ca_cluster_id = CLUSTER_ID_1001 and
   ca_frequency_id = FREQUENCY_ID_1001;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1001, CLUSTER_ID_1001, FREQUENCY_ID_1001,
      MIN_VAL_1001, MAX_VAL_1001);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1002 and ca_cluster_id = CLUSTER_ID_1002 and
   ca_frequency_id = FREQUENCY_ID_1001;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1002, CLUSTER_ID_1002, FREQUENCY_ID_1001,
      MIN_VAL_1002, MAX_VAL_1002);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1003 and ca_cluster_id = CLUSTER_ID_1003 and
   ca_frequency_id = FREQUENCY_ID_1001;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1003, CLUSTER_ID_1003, FREQUENCY_ID_1001,
      MIN_VAL_1003, MAX_VAL_1003);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1004 and ca_cluster_id = CLUSTER_ID_1004 and
   ca_frequency_id = FREQUENCY_ID_1001;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1004, CLUSTER_ID_1004, FREQUENCY_ID_1001,
      MIN_VAL_1004, MAX_VAL_1004);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1005 and ca_cluster_id = CLUSTER_ID_1001 and
   ca_frequency_id = FREQUENCY_ID_1002;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1005, CLUSTER_ID_1001, FREQUENCY_ID_1002,
      MIN_VAL_1005, MAX_VAL_1005);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1006 and ca_cluster_id = CLUSTER_ID_1002 and
   ca_frequency_id = FREQUENCY_ID_1002;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1006, CLUSTER_ID_1002, FREQUENCY_ID_1002,
      MIN_VAL_1006, MAX_VAL_1006);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1007 and ca_cluster_id = CLUSTER_ID_1003 and
   ca_frequency_id = FREQUENCY_ID_1002;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1007, CLUSTER_ID_1003, FREQUENCY_ID_1002,
      MIN_VAL_1007, MAX_VAL_1007);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1008 and ca_cluster_id = CLUSTER_ID_1004 and
   ca_frequency_id = FREQUENCY_ID_1002;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1008, CLUSTER_ID_1004, FREQUENCY_ID_1002,
      MIN_VAL_1008, MAX_VAL_1008);
   end if;

   select count(*) into row_count from ca_cluster_interval
   where id = CI_ID_1009 and ca_cluster_id = CLUSTER_ID_1004 and
   ca_frequency_id = FREQUENCY_ID_1003;
   if row_count = ZERO then
      insert into ca_cluster_interval (id, ca_cluster_id, ca_frequency_id,
      min_val, max_val)
      values(CI_ID_1009, CLUSTER_ID_1004, FREQUENCY_ID_1003,
      MIN_VAL_1009, MAX_VAL_1009);
   end if;

  exception
    when others then
      raise;
  end set_cluster_interval;

  procedure set_cycle
  is

    CY_ID_1001     constant  pls_integer := 1001;
    CY_ID_1002     constant  pls_integer := 1002;
    CY_ID_1003     constant  pls_integer := 1003;
    CY_ID_1004     constant  pls_integer := 1004;
    CY_ID_1005     constant  pls_integer := 1005;
    CY_ID_1006     constant  pls_integer := 1006;
    CY_ID_1007     constant  pls_integer := 1007;

    CYCLE_NAME_1001   constant varchar2(512) := 'Ny kund';
    CYCLE_NAME_1002   constant varchar2(512) := 'Ej aktiverad';
    CYCLE_NAME_1003   constant varchar2(512) := 'Passiv';
    CYCLE_NAME_1004   constant varchar2(512) := 'Churnad';
    CYCLE_NAME_1005   constant varchar2(512) := 'Tappar';
    CYCLE_NAME_1006   constant varchar2(512) := 'Ökar';
    CYCLE_NAME_1007   constant varchar2(512) := 'Oförändrad';

    ASC_DESC_PERCENTAGE_1005     constant  number := 0.3;
    ASC_DESC_PERCENTAGE_1006     constant  number := 0.3;

  begin
   select count(*) into row_count from ca_cycle
   where id = CY_ID_1001 and cycle_name = CYCLE_NAME_1001;
   if row_count = ZERO then
      insert into ca_cycle (id, cycle_name)
      values(CY_ID_1001, CYCLE_NAME_1001);
   end if;
 
   select count(*) into row_count from ca_cycle
   where id = CY_ID_1002 and cycle_name = CYCLE_NAME_1002;
   if row_count = ZERO then
      insert into ca_cycle (id, cycle_name)
      values(CY_ID_1002, CYCLE_NAME_1002);
   end if;

   select count(*) into row_count from ca_cycle
   where id = CY_ID_1003 and cycle_name = CYCLE_NAME_1003;
   if row_count = ZERO then
      insert into ca_cycle (id, cycle_name)
      values(CY_ID_1003, CYCLE_NAME_1003);
   end if;

   select count(*) into row_count from ca_cycle
   where id = CY_ID_1004 and cycle_name = CYCLE_NAME_1004;
   if row_count = ZERO then
      insert into ca_cycle (id, cycle_name)
      values(CY_ID_1004, CYCLE_NAME_1004);
   end if;

   select count(*) into row_count from ca_cycle
   where id = CY_ID_1005 and cycle_name = CYCLE_NAME_1005;
   if row_count = ZERO then
      insert into ca_cycle (id, cycle_name, asc_desc_percentage)
      values(CY_ID_1005, CYCLE_NAME_1005, ASC_DESC_PERCENTAGE_1005);
   end if;

   select count(*) into row_count from ca_cycle
   where id = CY_ID_1006 and cycle_name = CYCLE_NAME_1006;
   if row_count = ZERO then
      insert into ca_cycle (id, cycle_name, asc_desc_percentage)
      values(CY_ID_1006, CYCLE_NAME_1006, ASC_DESC_PERCENTAGE_1006);
   end if;

   select count(*) into row_count from ca_cycle
   where id = CY_ID_1007 and cycle_name = CYCLE_NAME_1007;
   if row_count = ZERO then
      insert into ca_cycle (id, cycle_name)
      values(CY_ID_1007, CYCLE_NAME_1007);
   end if;

  exception
    when others then
      raise;
  end set_cycle;

  procedure set_luck_skill
  is

    LS_ID_1001     constant  pls_integer := 1001;
    LS_ID_1002     constant  pls_integer := 1002;
    LS_ID_1003     constant  pls_integer := 1003;

    LUCK_SKILL_NAME_1001   constant varchar2(512) := 'Turspelare';
    LUCK_SKILL_NAME_1002   constant varchar2(512) := 'Skicklighetsspelare';
    LUCK_SKILL_NAME_1003   constant varchar2(512) := 'Andelsspelare';

  begin
   select count(*) into row_count from ca_luck_skill
   where id = LS_ID_1001 and luck_skill_name = LUCK_SKILL_NAME_1001;
   if row_count = ZERO then
      insert into ca_luck_skill (id, luck_skill_name)
      values(LS_ID_1001, LUCK_SKILL_NAME_1001);
   end if;

   select count(*) into row_count from ca_luck_skill
   where id = LS_ID_1002 and luck_skill_name = LUCK_SKILL_NAME_1002;
   if row_count = ZERO then
      insert into ca_luck_skill (id, luck_skill_name)
      values(LS_ID_1002, LUCK_SKILL_NAME_1002);
   end if;
   
   select count(*) into row_count from ca_luck_skill
   where id = LS_ID_1003 and luck_skill_name = LUCK_SKILL_NAME_1003;
   if row_count = ZERO then
      insert into ca_luck_skill (id, luck_skill_name)
      values(LS_ID_1003, LUCK_SKILL_NAME_1003);
   end if;   

  exception
    when others then
      raise;
  end set_luck_skill;

  procedure set_revenue_interval
  is
     l_exist number;
  begin
       select count(*) into l_exist from spst.ca_revenue_interval where id = 1001;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1001,1,1000000,99999999999999999999999999999999999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1002;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1002,2,500000,999999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1003;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1003,3,250000,499999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id =  1004;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1004,4,100000,249999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id =  1005;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1005,5,50000,99999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id =  1006;
       if l_exist = 0 then

        Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1006,6,25000,49999);

       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1007;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1007,7,10000,24999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1008;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1008,8,5000,9999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1009;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1009,9,2500,4999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1010;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1010,10,1000,2499);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id =  1011;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1011,11,500,999);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1012;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1012,12,250,499);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id =  1013;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1013,13,0.01,249);
       end if;

       select count(*) into l_exist from spst.ca_revenue_interval where id = 1014;
       if l_exist = 0 then
          Insert into CA_REVENUE_INTERVAL (ID,CLASS,REVENUE_FROM,REVENUE_TO) values (1014,14,0,0);
       end if;
  exception
    when others then
      raise;
  end set_revenue_interval;

  procedure set_gallop_and_vr
  is

    GV_ID_1001     constant  pls_integer := 1001;
    GV_ID_1002     constant  pls_integer := 1002;

    GALLOP_AND_VR_NAME_1001   constant varchar2(512) := 'Galopp-spelare';
    GALLOP_AND_VR_NAME_1002   constant varchar2(512) := 'VR-spelare';

  begin
   select count(*) into row_count from ca_gallop_and_vr
   where id = GV_ID_1001 and gallop_and_vr_name = GALLOP_AND_VR_NAME_1001;
   if row_count = ZERO then
      insert into ca_gallop_and_vr (id, gallop_and_vr_name)
      values(GV_ID_1001, GALLOP_AND_VR_NAME_1001);
   end if;

   select count(*) into row_count from ca_gallop_and_vr
   where id = GV_ID_1002 and gallop_and_vr_name = GALLOP_AND_VR_NAME_1002;
   if row_count = ZERO then
      insert into ca_gallop_and_vr (id, gallop_and_vr_name)
      values(GV_ID_1002, GALLOP_AND_VR_NAME_1002);
   end if;

  exception
    when others then
      raise;
  end set_gallop_and_vr;

  procedure set_channel
  is

    CH_ID_1001     constant  pls_integer := 1001;
    CH_ID_1002     constant  pls_integer := 1002;
    CH_ID_1003     constant  pls_integer := 1003;
    CH_ID_1004     constant  pls_integer := 1004;

    CHANNEL_NAME_1001   constant varchar2(512) := 'Online-kund';
    CHANNEL_NAME_1002   constant varchar2(512) := 'Butikskund';
    CHANNEL_NAME_1003   constant varchar2(512) := 'Båda kanalerna';
    CHANNEL_NAME_1004   constant varchar2(512) := 'Hemmabutik';

  begin
   select count(*) into row_count from ca_channel
   where id = CH_ID_1001 and channel_name = CHANNEL_NAME_1001;
   if row_count = ZERO then
      insert into ca_channel (id, channel_name)
      values(CH_ID_1001, CHANNEL_NAME_1001);
   end if;

   select count(*) into row_count from ca_channel
   where id = CH_ID_1002 and channel_name = CHANNEL_NAME_1002;
   if row_count = ZERO then
      insert into ca_channel (id, channel_name)
      values(CH_ID_1002, CHANNEL_NAME_1002);
   end if;

   select count(*) into row_count from ca_channel
   where id = CH_ID_1003 and channel_name = CHANNEL_NAME_1003;
   if row_count = ZERO then
      insert into ca_channel (id, channel_name)
      values(CH_ID_1003, CHANNEL_NAME_1003);
   end if;

   select count(*) into row_count from ca_channel
   where id = CH_ID_1004 and channel_name = CHANNEL_NAME_1004;
   if row_count = ZERO then
      insert into ca_channel (id, channel_name)
      values(CH_ID_1004, CHANNEL_NAME_1004);
   end if;

  exception
    when others then
      raise;
  end set_channel;


  procedure set_together
  is

    CT_ID_1001     constant  pls_integer := 1001;
    CT_ID_1002     constant  pls_integer := 1002;
    CT_ID_1003     constant  pls_integer := 1003;
    CT_ID_1004     constant  pls_integer := 1004;

    TOGETHER_NAME_1001   constant varchar2(512) := 'Aktiv lagkapten';
    TOGETHER_NAME_1002   constant varchar2(512) := 'Inaktiv lagkapten';
    TOGETHER_NAME_1003   constant varchar2(512) := 'Aktiv lagmedlem';
    TOGETHER_NAME_1004   constant varchar2(512) := 'Inaktiv lagmedlem';

  begin
   select count(*) into row_count from ca_together
   where id = CT_ID_1001 and together_name = TOGETHER_NAME_1001;
   if row_count = ZERO then
      insert into ca_together (id, together_name)
      values(CT_ID_1001, TOGETHER_NAME_1001);
   end if;

   select count(*) into row_count from ca_together
   where id = CT_ID_1002 and together_name = TOGETHER_NAME_1002;
   if row_count = ZERO then
      insert into ca_together (id, together_name)
      values(CT_ID_1002, TOGETHER_NAME_1002);
   end if;

   select count(*) into row_count from ca_together
   where id = CT_ID_1003 and together_name = TOGETHER_NAME_1003;
   if row_count = ZERO then
      insert into ca_together (id, together_name)
      values(CT_ID_1003, TOGETHER_NAME_1003);
   end if;

   select count(*) into row_count from ca_together
   where id = CT_ID_1004 and together_name = TOGETHER_NAME_1004;
   if row_count = ZERO then
      insert into ca_together (id, together_name)
      values(CT_ID_1004, TOGETHER_NAME_1004);
   end if;


  exception
    when others then
      raise;
  end set_together;


begin

  set_cluster;
  set_priority;
  set_cluster_concept;
  set_frequency;
  set_cluster_interval;
  set_cycle;
  set_luck_skill;
  set_revenue_interval;
  set_gallop_and_vr;
  set_channel;
  set_together;

exception
   when others then
      dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
      raise;
end set_meta_data_repository;
------------------------------------------------------------------------------
procedure insert_one_date_time_day(p_date date) is
  MODULE_NAME            constant varchar2(30) := 'insert_one_date_time_day';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
begin

  if p_date is not null then null; else return; end if;

  insert into spst.ca_time_day (
    id,full_date,year,day_of_week,day_of_month,day_of_quarter,number_of_week,
    number_of_month,number_of_quarter,name_of_day,name_of_month,name_of_quarter,
    year_month,year_week,year_quarter,is_last_day_of_month,is_last_day_of_week,
    is_last_day_of_quarter)
  select
      ca_time_day_seq.nextval           as id,
      p_date                            as full_date,
      to_number(to_char(p_date,'YYYY')) as year,
      to_number(to_char(p_date,'d'))    as day_of_week,
      to_number(to_char(p_date,'dd'))   as day_of_month,
      trunc(p_date)-trunc(p_date,'q')+1 as day_of_quarter,
      to_number(to_char(p_date,'iw'))   as number_of_week,
      to_number(to_char(p_date,'mm'))   as number_of_month,
      to_number(to_char(p_date,'q'))    as number_of_quarter,
      to_char(p_date,'Day')             as name_of_day,
      to_char(p_date,'Month')           as name_of_month,
      'Q'||to_char(p_date,'q')          as name_of_quarter,
      to_char(p_date,'yyyymm')          as year_month,
      to_char(p_date,'iyyyiw')          as year_week,
      to_char(p_date,'YYYY')||'Q'||
          to_char(p_date,'q')           as year_quarter,
      case when trunc(p_date)=trunc(last_day(p_date)) then
         1
      else 0 end                        as is_last_day_of_month,
      case when to_number(to_char(p_date,'d'))=7 then
         1
      else 0 end                        as is_last_day_of_week,
      case when trunc(p_date)=trunc(add_months(p_date, +3), 'Q')-1 then
         1
      else 0 end                        as is_last_day_of_quarter
  from dual;

exception
   when dup_val_on_index then
        null;
   when others then
      dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
      raise;
end insert_one_date_time_day;
------------------------------------------------------------------------------
function get_time_day_id(p_date date) return number
  result_cache as
  l_id number;
  pragma udf;
begin
  select id into l_id from ca_time_day where full_date=p_date;
  return l_id;

exception
  when others then
    raise;
end get_time_day_id;
------------------------------------------------------------------------------
function get_last_day_of_week(p_date date) return date as
  l_date date;
begin
  select full_date
    into l_date
    from ca_time_day
   where year_week = (select year_week from ca_time_day where full_date=p_date)
     and is_last_day_of_week = ONE;

  return l_date;

exception
  when others then
    raise;
end get_last_day_of_week;
------------------------------------------------------------------------------
function get_number_of_days_in_week(p_date date) return number
as
  l_number_of_days_in_week number;
begin
        select count(distinct ctd.full_date)
          into l_number_of_days_in_week
          from customer_activity ca,
               ca_time_day ctd
         where ctd.id=ca.ca_time_day_id
           and ctd.YEAR_week=(select year_week from ca_time_day where full_date=p_date);

        return l_number_of_days_in_week;
 exception
    when others then
         raise;
end get_number_of_days_in_week;
------------------------------------------------------------------------------
function get_num_of_game_days(p_priority number) return number
  result_cache
as
  l_num_of_game_days number;
  pragma udf;
begin
        select num_of_game_days
          into l_num_of_game_days
          from ca_priority
         where priority = p_priority;

        return l_num_of_game_days;
 exception
    when others then
         raise;
end get_num_of_game_days;
------------------------------------------------------------------------------
function get_percentage(p_priority number) return number
  result_cache
as
  l_percentage number;
  pragma udf;
begin
        select percentage
          into l_percentage
          from ca_priority
         where priority = p_priority;

        return l_percentage;
 exception
    when others then
         raise;
end get_percentage;
------------------------------------------------------------------------------
function is_customer_revenue(time_day_id number) return boolean
as
  count_rows  pls_integer;

begin
  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
   into count_rows
   from customer_revenue
  where ca_time_day_id = time_day_id
    and rownum < TWO;

  return (count_rows = ONE);

end is_customer_revenue;
------------------------------------------------------------------------------
function is_num_of_game_days(time_day_id number) return boolean
as
  count_rows  pls_integer;

begin
  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
   into count_rows
   from num_of_game_days
  where ca_time_day_id = time_day_id
    and rownum < TWO;

  return (count_rows = ONE);

end is_num_of_game_days;
------------------------------------------------------------------------------
function is_num_of_game_days_total(time_day_id number) return boolean
as
  count_rows  pls_integer;
begin
  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
   into count_rows
   from num_of_game_days_total
  where ca_time_day_id = time_day_id
    and rownum < TWO;

  return (count_rows = ONE);

end is_num_of_game_days_total;
------------------------------------------------------------------------------
function is_program_concept_priority(p_program_concept_id number, p_priority number) return number
  result_cache
as
  count_rows  pls_integer;
  pragma udf;
begin
  select /*+ first_rows(1) */ count(ca_cluster_concept.id)
  into count_rows
  from ca_cluster_concept, ca_priority
  where  ca_priority_id = ca_priority.id and
         ca_program_concept_id = p_program_concept_id and
         priority = p_priority and rownum < TWO;

  return count_rows;

end is_program_concept_priority;
------------------------------------------------------------------------------
function is_nogd_trot_concept(time_day_id number) return boolean
as
  count_rows  pls_integer;
begin
  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
   into count_rows
   from num_of_game_days_trot_concept
  where ca_time_day_id = time_day_id
    and rownum < TWO;

  return (count_rows = ONE);

end is_nogd_trot_concept;
------------------------------------------------------------------------------
function get_bet_method_group_id(p_orig_id number) return number
  result_cache as
  pragma udf;
  l_id number;
begin
  select id into l_id from ca_bet_method_group where orig_id=p_orig_id;
  return l_id;
end get_bet_method_group_id;
------------------------------------------------------------------------------
function get_race_type_id(p_prg_date date, p_host_track varchar2) return number
  result_cache as
  l_race_type_id number;
  pragma udf;
begin
  select case when pbv.tavlingstyp_id in (40,54) then
           TROT_ID
         else
           GALLOP_ID
         end race_type_id
    into l_race_type_id
    from programattribut_bas_vy pbv
   where pbv.prgdate=p_prg_date
     and pbv.host_track=p_host_track;
  return l_race_type_id;

end get_race_type_id;
------------------------------------------------------------------------------
function get_program_concept_id(p_prg_date date, p_host_track varchar2) return number
  result_cache as
  l_program_concept_id number;
  pragma udf;
begin
  select cpc.id
    into l_program_concept_id
    from programattribut_bas_vy pbv, ca_program_concept cpc
   where pbv.koncept_id=cpc.orig_id
     and pbv.prgdate=p_prg_date
     and pbv.host_track=p_host_track;
  return l_program_concept_id;

end get_program_concept_id;
------------------------------------------------------------------------------
function get_channel_type_id(p_channel_type varchar2) return number
  result_cache as
  l_channel_type_id number;
  pragma udf;
begin
  select id
    into l_channel_type_id
    from ca_channel_type
   where channel_type=p_channel_type;

  return l_channel_type_id;

end get_channel_type_id;
------------------------------------------------------------------------------

procedure init_customer_revenue(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is
  MODULE_NAME            constant varchar2(30) := 'init_customer_revenue';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  insert into customer_revenue(
         CA_TIME_DAY_ID,
         PUNTER_PUNTER_ID,
         CA_GAME_TYPE_ID,
         CA_RACE_TYPE_ID,
         CA_PROGRAM_CONCEPT_ID,
         CA_CHANNEL_TYPE_ID,
         CA_BET_METHOD_GROUP_ID,
         TECH_CHANNEL_ID,
         REVENUE_WTD,
         REVENUE_R13_WEEK,
         REVENUE_R52_WEEK,
         REVENUE_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               PUNTER_PUNTER_ID,
               CA_GAME_TYPE_ID,
               CA_RACE_TYPE_ID,
               CA_PROGRAM_CONCEPT_ID,
               CA_CHANNEL_TYPE_ID,
               CA_BET_METHOD_GROUP_ID,
               TECH_CHANNEL_ID,
               REVENUE_WTD,
               REVENUE_R13_WEEK,
               REVENUE_R52_WEEK,
               REVENUE_TOTAL
        from (
                select p.punter_key,
                       ca_game_type_id,
                       ca_race_type_id,
                       ca_program_concept_id,
                       ca_channel_type_id,
                       ca_bet_method_group_id,
                       tech_channel_id,
                       max(p.punter_id) punter_punter_id,
                       sum(case when ctd.YEAR_WEEK = p_end_week then ca.revenue else 0 end) as REVENUE_WTD,
                       sum(case when ctd.YEAR_WEEK >= p_start_week_r13 then ca.revenue else 0 end) as REVENUE_R13_WEEK,
                       sum(case when ctd.YEAR_WEEK >= p_start_week_r52 then ca.revenue else 0 end) as REVENUE_R52_WEEK,
                       sum(ca.revenue) as REVENUE_TOTAL
                  from customer_activity ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ctd.year_week between p_start_week AND p_end_week
                 group by p.punter_key,
                           ca.ca_game_type_id,
                           ca.ca_race_type_id,
                           ca.ca_program_concept_id,
                           ca.ca_channel_type_id,
                           ca.ca_bet_method_group_id,
                           ca.tech_channel_id);
                           
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);
exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_customer_revenue;

------------------------------------------------------------------------------
procedure init_nogd(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is
  MODULE_NAME            constant varchar2(30) := 'init_nogd';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  insert into num_of_game_days(
         CA_TIME_DAY_ID,
         PUNTER_PUNTER_ID,
         CA_GAME_TYPE_ID,
         CA_RACE_TYPE_ID,
         CA_PROGRAM_CONCEPT_ID,
         CA_CHANNEL_TYPE_ID,
         CA_BET_METHOD_GROUP_ID,
         TECH_CHANNEL_ID,
         NUM_OF_GAME_DAYS_WTD,
         NUM_OF_GAME_DAYS_R13_WEEK,
         NUM_OF_GAME_DAYS_R52_WEEK,
         NUM_OF_GAME_DAYS_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               PUNTER_PUNTER_ID,
               CA_GAME_TYPE_ID,
               CA_RACE_TYPE_ID,
               CA_PROGRAM_CONCEPT_ID,
               CA_CHANNEL_TYPE_ID,
               CA_BET_METHOD_GROUP_ID,
               TECH_CHANNEL_ID,
               NUM_OF_GAME_DAYS_WTD,
               NUM_OF_GAME_DAYS_R13_WEEK,
               NUM_OF_GAME_DAYS_R52_WEEK,
               NUM_OF_GAME_DAYS_TOTAL
        from (
                select p.punter_key,
                       ca_game_type_id,
                       ca_race_type_id,
                       ca_program_concept_id,
                       ca_channel_type_id,
                       ca_bet_method_group_id,
                       tech_channel_id,
                       max(p.punter_id) punter_punter_id,
                       count(distinct case when ctd.YEAR_WEEK = p_end_week then ctd.full_date else null end) as NUM_OF_GAME_DAYS_WTD,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r13 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R13_WEEK,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r52 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R52_WEEK,
                       count(distinct ctd.full_date) as NUM_OF_GAME_DAYS_TOTAL
                  from customer_activity ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ctd.year_week between p_start_week AND p_end_week
                 group by p.punter_key,
                           ca.ca_game_type_id,
                           ca.ca_race_type_id,
                           ca.ca_program_concept_id,
                           ca.ca_channel_type_id,
                           ca.ca_bet_method_group_id,
                           ca.tech_channel_id);
                           
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);                           
                           
exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_nogd;

------------------------------------------------------------------------------

procedure init_nogd_total(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is
  MODULE_NAME            constant varchar2(30) := 'init_nogd_total';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  insert into num_of_game_days_total(
         CA_TIME_DAY_ID,
         PUNTER_PUNTER_ID,
         NUM_OF_GAME_DAYS_WTD,
         NUM_OF_GAME_DAYS_R13_WEEK,
         NUM_OF_GAME_DAYS_R52_WEEK,
         NUM_OF_GAME_DAYS_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               PUNTER_PUNTER_ID,
               NUM_OF_GAME_DAYS_WTD,
               NUM_OF_GAME_DAYS_R13_WEEK,
               NUM_OF_GAME_DAYS_R52_WEEK,
               NUM_OF_GAME_DAYS_TOTAL
        from (
                select p.punter_key,
                       max(p.punter_id) punter_punter_id,
                       count(distinct case when ctd.YEAR_WEEK = p_end_week then ctd.full_date else null end) as NUM_OF_GAME_DAYS_WTD,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r13 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R13_WEEK,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r52 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R52_WEEK,
                       count(distinct ctd.full_date) as NUM_OF_GAME_DAYS_TOTAL
                  from customer_activity ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ctd.year_week between p_start_week AND p_end_week
                 group by p.punter_key);

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);
                 
                 
exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_nogd_total;

------------------------------------------------------------------------------

procedure init_nogd_trot_concept(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is
  MODULE_NAME            constant varchar2(30) := 'init_nogd_trot_concept';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  insert into num_of_game_days_trot_concept(
         CA_TIME_DAY_ID,
         CA_PROGRAM_CONCEPT_ID,
         PUNTER_PUNTER_ID,
         NUM_OF_GAME_DAYS_WTD,
         NUM_OF_GAME_DAYS_R13_WEEK,
         NUM_OF_GAME_DAYS_R52_WEEK,
         NUM_OF_GAME_DAYS_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               CA_PROGRAM_CONCEPT_ID,
               PUNTER_PUNTER_ID,
               NUM_OF_GAME_DAYS_WTD,
               NUM_OF_GAME_DAYS_R13_WEEK,
               NUM_OF_GAME_DAYS_R52_WEEK,
               NUM_OF_GAME_DAYS_TOTAL
        from (
                select p.punter_key,
                       ca_program_concept_id,
                       max(p.punter_id) punter_punter_id,
                       count(distinct case when ctd.YEAR_WEEK = p_end_week then ctd.full_date else null end) as NUM_OF_GAME_DAYS_WTD,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r13 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R13_WEEK,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r52 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R52_WEEK,
                       count(distinct ctd.full_date) as NUM_OF_GAME_DAYS_TOTAL
                  from customer_activity ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ca_race_type_id = TROT_ID
                   and ctd.year_week between p_start_week AND p_end_week
                 group by p.punter_key, ca_program_concept_id);
                 
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);                 

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_nogd_trot_concept;

------------------------------------------------------------------------------
procedure init_nogd_race_type(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is

  MODULE_NAME            constant varchar2(30) := 'init_nogd_race_type';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;  

begin
    ld_start_time:=sysdate;
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  insert into num_of_game_days_race_type(
         CA_TIME_DAY_ID,
         CA_RACE_TYPE_ID,
         PUNTER_PUNTER_ID,
         NUM_OF_GAME_DAYS_WTD,
         NUM_OF_GAME_DAYS_R13_WEEK,
         NUM_OF_GAME_DAYS_R52_WEEK,
         NUM_OF_GAME_DAYS_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               CA_RACE_TYPE_ID,
               PUNTER_PUNTER_ID,
               NUM_OF_GAME_DAYS_WTD,
               NUM_OF_GAME_DAYS_R13_WEEK,
               NUM_OF_GAME_DAYS_R52_WEEK,
               NUM_OF_GAME_DAYS_TOTAL
        from (
                select p.punter_key,
                       ca_race_type_id,
                       max(p.punter_id) punter_punter_id,
                       count(distinct case when ctd.YEAR_WEEK = p_end_week then ctd.full_date else null end) as NUM_OF_GAME_DAYS_WTD,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r13 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R13_WEEK,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r52 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R52_WEEK,
                       count(distinct ctd.full_date) as NUM_OF_GAME_DAYS_TOTAL
                  from customer_activity ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ctd.year_week between p_start_week AND p_end_week
                 group by p.punter_key, ca_race_type_id);

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);                 
                 

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_nogd_race_type;

------------------------------------------------------------------------------

procedure init_nogd_hr_race_type(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is

  MODULE_NAME            constant varchar2(30) := 'init_nogd_hr_race_type';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);
                             
  insert into num_of_game_days_hr_race_type(
         CA_TIME_DAY_ID,
         CA_RACE_TYPE_ID,
         PUNTER_PUNTER_ID,
         NUM_OF_GAME_DAYS_WTD,
         NUM_OF_GAME_DAYS_R13_WEEK,
         NUM_OF_GAME_DAYS_R52_WEEK,
         NUM_OF_GAME_DAYS_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               CA_RACE_TYPE_ID,
               PUNTER_PUNTER_ID,
               NUM_OF_GAME_DAYS_WTD,
               NUM_OF_GAME_DAYS_R13_WEEK,
               NUM_OF_GAME_DAYS_R52_WEEK,
               NUM_OF_GAME_DAYS_TOTAL
        from (
                select p.punter_key,
                       ca_race_type_id,
                       max(p.punter_id) punter_punter_id,
                       count(distinct case when ctd.YEAR_WEEK = p_end_week then ctd.full_date else null end) as NUM_OF_GAME_DAYS_WTD,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r13 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R13_WEEK,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r52 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R52_WEEK,
                       count(distinct ctd.full_date) as NUM_OF_GAME_DAYS_TOTAL
                  from customer_activity ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ctd.year_week between p_start_week AND p_end_week
                   and ca.ca_game_type_id = HORSE_RACING_ID
                 group by p.punter_key, ca_race_type_id);
                 
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_nogd_hr_race_type;

------------------------------------------------------------------------------

procedure init_nogd_game_type(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is

  MODULE_NAME            constant varchar2(30) := 'init_nogd_game_type';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  insert into num_of_game_days_game_type(
         CA_TIME_DAY_ID,
         CA_GAME_TYPE_ID,
         PUNTER_PUNTER_ID,
         NUM_OF_GAME_DAYS_WTD,
         NUM_OF_GAME_DAYS_R13_WEEK,
         NUM_OF_GAME_DAYS_R52_WEEK,
         NUM_OF_GAME_DAYS_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               CA_GAME_TYPE_ID,
               PUNTER_PUNTER_ID,
               NUM_OF_GAME_DAYS_WTD,
               NUM_OF_GAME_DAYS_R13_WEEK,
               NUM_OF_GAME_DAYS_R52_WEEK,
               NUM_OF_GAME_DAYS_TOTAL
        from (
                select p.punter_key,
                       ca_game_type_id,
                       max(p.punter_id) punter_punter_id,
                       count(distinct case when ctd.YEAR_WEEK = p_end_week then ctd.full_date else null end) as NUM_OF_GAME_DAYS_WTD,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r13 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R13_WEEK,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r52 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R52_WEEK,
                       count(distinct ctd.full_date) as NUM_OF_GAME_DAYS_TOTAL
                  from customer_activity ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ctd.year_week between p_start_week AND p_end_week
                 group by p.punter_key, ca_game_type_id);

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_nogd_game_type;

------------------------------------------------------------------------------
procedure init_nogd_channel_rev(p_start_week pls_integer, p_end_week pls_integer, p_start_week_r52 pls_integer, p_start_week_r13 pls_integer, p_date date) is
  MODULE_NAME            constant varchar2(30) := 'init_nogd_channel_rev';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;  
begin
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

  insert into num_of_game_days_channel_rev(
         CA_TIME_DAY_ID,
         CHANNEL_CHANNEL_ID,
         PARTNER_CHANNEL_ID,
         PUNTER_KEY,
         PUNTER_PUNTER_ID,
         NUM_OF_GAME_DAYS_WTD,
         NUM_OF_GAME_DAYS_R13_WEEK,
         NUM_OF_GAME_DAYS_R52_WEEK,
         NUM_OF_GAME_DAYS_TOTAL,
         REVENUE_WTD,
         REVENUE_R13_WEEK,
         REVENUE_R52_WEEK,
         REVENUE_TOTAL
  )
        select get_time_day_id(p_date) as CA_TIME_DAY_ID,
               CHANNEL_CHANNEL_ID,
               PARTNER_CHANNEL_ID,
               PUNTER_KEY,
               PUNTER_PUNTER_ID,
               NUM_OF_GAME_DAYS_WTD,
               NUM_OF_GAME_DAYS_R13_WEEK,
               NUM_OF_GAME_DAYS_R52_WEEK,
               NUM_OF_GAME_DAYS_TOTAL,
               REVENUE_WTD,
               REVENUE_R13_WEEK,
               REVENUE_R52_WEEK,
               REVENUE_TOTAL
        from (
                select p.punter_key,
                       channel_channel_id,
                       partner_channel_id,
                       max(max(p.punter_id)) over (partition by p.punter_key) punter_punter_id,
                       count(distinct case when ctd.YEAR_WEEK = p_end_week then ctd.full_date else null end) as NUM_OF_GAME_DAYS_WTD,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r13 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R13_WEEK,
                       count(distinct case when ctd.YEAR_WEEK >= p_start_week_r52 then ctd.full_date else null end) as NUM_OF_GAME_DAYS_R52_WEEK,
                       count(distinct ctd.full_date) as NUM_OF_GAME_DAYS_TOTAL,
                       sum(case when ctd.YEAR_WEEK = p_end_week then ca.revenue else 0 end) as REVENUE_WTD,
                       sum(case when ctd.YEAR_WEEK >= p_start_week_r13 then ca.revenue else 0 end) as REVENUE_R13_WEEK,
                       sum(case when ctd.YEAR_WEEK >= p_start_week_r52 then ca.revenue else 0 end) as REVENUE_R52_WEEK,
                       sum(ca.revenue) as REVENUE_TOTAL
                  from customer_activity2 ca,
                       ca_time_day ctd,
                       punter p
                 where ctd.id=ca.ca_time_day_id
                   and ca.punter_punter_id=p.punter_id
                   and p.kontotyp=TYPE_OF_ACCOUNT
                   and ctd.year_week between p_start_week AND p_end_week
                 group by p.punter_key, channel_channel_id, partner_channel_id);


            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init_nogd_channel_rev;

------------------------------------------------------------------------------
procedure load_customer_activity(p_date date) is

  MODULE_NAME            constant varchar2(30) := 'load_customer_activity';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  ld_start                varchar2(30);
  ld_end                  varchar2(30);

begin

    ld_start_time:=sysdate;
    ld_start:=to_char(p_date-12,'yyyymmdd');
    ld_end:=to_char(p_date+1,'yyyymmdd');
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

  insert into customer_activity(
      ca_time_day_id,
      punter_punter_id,
      ca_game_type_id,
      ca_race_type_id,
      ca_program_concept_id,
      ca_channel_type_id,
      ca_bet_method_group_id,
      tech_channel_id,
      revenue)
  with
      day_rev as
      (select 
          get_time_day_id(prg.prg_date)       as ca_time_day_id,
          p.punter_id                         as punter_punter_id,
          prg.host_track,
          case when prg.track_int=127 then
              VIRTUAL_RACING_ID
          else
              HORSE_RACING_ID
          end                                 as ca_game_type_id,
          get_race_type_id(prg.prg_date, prg.host_track) as ca_race_type_id,
          get_program_concept_id(prg.prg_date, prg.host_track) as ca_program_concept_id,
          get_channel_type_id(ch.can_type) as ca_channel_type_id,
          get_bet_method_group_id(bm.betmethod_group_id) as ca_bet_method_group_id,
          case when ch.can_type in ('Ombud','Internationella Ombud') then
            7 -- Ombud, självbetjäning i ombudsmiljö
          else
            bfa.tc_id
          end as tech_channel_id,
          sum(bfa.net_rev) as Revenue
      from betfact_agg bfa,
                   program prg,
                   channel ch,
                   betmethod bm,
                   punter p 
      where bfa.ba_program_id=prg.program_id
       and bfa.ba_channel_id=ch.channel_id
       and bfa.ba_bet_method_id=bm.bet_method_id
       and bfa.ba_punter_id=p.punter_id
       and prg.prg_date=to_char(p_date,'yyyymmdd')
       and p.punter_id != 0
       and p.kontotyp='PERSON'
       and bfa.ba_time_key between ld_start and ld_end
      group by p.punter_id,
              prg.prg_date,
              prg.host_track,
              prg.track_int,
              ch.can_type,
              bm.betmethod_group_id, 
              bfa.tc_id)
  select ca_time_day_id,
        punter_punter_id,
        ca_game_type_id,
        coalesce(ca_race_type_id,-1) as ca_race_type_id,
        coalesce(ca_program_concept_id,-1) as ca_program_concept_id, -- If concept is null
        ca_channel_type_id,
        ca_bet_method_group_id,
        coalesce(tech_channel_id,0) as tech_channel_id,
        sum(revenue) revenue
   from day_rev
  group by ca_time_day_id,
           punter_punter_id,
           ca_game_type_id,
           ca_race_type_id,
           ca_program_concept_id,
           ca_channel_type_id,
           ca_bet_method_group_id,
           tech_channel_id;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_customer_activity;

------------------------------------------------------------------------------

procedure load_customer_activity2(p_date date) is

  MODULE_NAME            constant varchar2(30) := 'load_customer_activity2';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  ld_start                varchar2(30);
  ld_end                  varchar2(30);

begin
    ld_start_time:=sysdate;
    ld_start:=to_char(p_date-12,'yyyymmdd');
    ld_end:=to_char(p_date+1,'yyyymmdd');    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

  insert into customer_activity2(
      ca_time_day_id,
      punter_punter_id,
      channel_channel_id,
      partner_channel_id,
      revenue)
  with
      day_rev as
      (select
          get_time_day_id(prg.prg_date)       as ca_time_day_id,
          p.punter_id                         as punter_punter_id,
          ch.channel_id                       as channel_channel_id,
          pc.partner_channel_id               as partner_channel_id,
          sum(bfa.net_rev)                    as revenue
      from betfact_agg bfa,
                   program prg,
                   channel ch,
                   partner_channel pc,
                   punter p
      where bfa.ba_program_id=prg.program_id
       and bfa.ba_channel_id=ch.channel_id
       and bfa.partner_channel_id=pc.partner_channel_id
       and bfa.ba_punter_id=p.punter_id
       and prg.prg_date=to_char(p_date,'yyyymmdd')
       and p.punter_id != 0
       and p.kontotyp='PERSON'
       and bfa.ba_time_key between ld_start and ld_end
      group by prg.prg_date,
                p.punter_id,
                ch.channel_id,
                pc.partner_channel_id)
  select ca_time_day_id,
        punter_punter_id,
        channel_channel_id,
        partner_channel_id,
        sum(revenue) revenue
   from day_rev
  group by ca_time_day_id,
           punter_punter_id,
           channel_channel_id,
           partner_channel_id;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_customer_activity2;
------------------------------------------------------------------------------

procedure load_cust_together_activity(p_date date) is

  MODULE_NAME            constant varchar2(30) := 'load_cust_together_activity';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

begin
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

        insert into spst.customer_together_activity (ca_time_day_id,
                                                     member_account_index,
                                                     player_account_index,
                                                     captain_account_index,
                                                     team_account_index,
                                                     num_of_file_plays,
                                                     num_of_retail_plays,
                                                     num_of_common_plays,
                                                     revenue)

        select get_time_day_id(prg.datum)  as ca_time_day_id,
               konto_kontoindex as member_account_index,
               konto_kontoindex_spellaggare as player_account_index,
               konto_kontoindex_lagledare as captain_account_index,
               tillsammans_lag_kontoindex as team_account_index,
               count(distinct gruppspelid) num_of_file_plays,
               count(case when tekniska_kanaler_id=20 then tekniska_kanaler_id end) num_of_retail_plays,
               count(distinct tsn)-count(case when tekniska_kanaler_id=20 then tekniska_kanaler_id end) num_of_common_plays,
               sum(belopp) revenue
          from mdb.speltransaktioner st, mdb.program prg
         where tekniska_kanaler_id in (19) --Together tc -- Bara tillsammans - ej ombudslag (20)
           and st.program_id = prg.id
           and konto_kontoindex is not null and konto_kontoindex_spellaggare is not null and konto_kontoindex_lagledare is not null
           and prg.datum = p_date
        group by prg.datum, konto_kontoindex, konto_kontoindex_spellaggare, konto_kontoindex_lagledare, tillsammans_lag_kontoindex;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);


 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_cust_together_activity;

------------------------------------------------------------------------------

procedure load_customer_revenue(p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

   l_test_date date;

  MODULE_NAME            constant varchar2(30) := 'load_customer_revenue';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  
 begin

    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

   -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from customer_revenue
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       -- ROLLING_LONG_WEEK
       select year_week into l_r_long_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
       -- ROLLING_SHORT_WEEK
       select year_week into l_r_short_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;
       -- last year_week
       select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       dbms_output.put_line('l_r_long_first_year_week: '||l_r_long_first_year_week);
       dbms_output.put_line('l_r_short_first_year_week: '||l_r_short_first_year_week);
       dbms_output.put_line('l_last_year_week: '||l_last_year_week);
    end if;

    if l_insert then
    
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));    
    
            insert into customer_revenue(ca_time_day_id,
                                                   punter_punter_id,
                                                   ca_game_type_id,
                                                   ca_race_type_id,
                                                   ca_program_concept_id,
                                                   ca_channel_type_id,
                                                   ca_bet_method_group_id,
                                                   tech_channel_id,
                                                   revenue_wtd,
                                                   revenue_r13_week,
                                                   revenue_r52_week,
                                                   revenue_total)
            with week_rev_act_week as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id,
                                   sum(revenue) as revenue_wtd_plus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_act_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id),
            week_rev_minus_rlong as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id,
                                   sum(revenue) as revenue_wtd_rlong_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_long_first_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id),
            week_rev_minus_rshort as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id,
                                   sum(revenue) as revenue_wtd_rshort_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_short_first_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id),
            week_act_join_long as (
                select coalesce(wr.punter_key,wrlong.punter_key) as punter_key,
                       coalesce(wr.punter_punter_id,wrlong.punter_punter_id) as punter_punter_id,
                       coalesce(wr.ca_game_type_id,wrlong.ca_game_type_id) as ca_game_type_id,
                       coalesce(wr.ca_race_type_id,wrlong.ca_race_type_id) as ca_race_type_id,
                       coalesce(wr.ca_program_concept_id,wrlong.ca_program_concept_id) as ca_program_concept_id,
                       coalesce(wr.ca_channel_type_id,wrlong.ca_channel_type_id) as ca_channel_type_id,
                       coalesce(wr.ca_bet_method_group_id,wrlong.ca_bet_method_group_id) as ca_bet_method_group_id,
                       coalesce(wr.tech_channel_id,wrlong.tech_channel_id) as tech_channel_id,
                       wr.revenue_wtd_plus as revenue_wtd_plus,
                       wrlong.revenue_wtd_rlong_minus as revenue_wtd_rlong_minus
                  from week_rev_act_week wr full outer join week_rev_minus_rlong wrlong
                 on (wr.punter_key=wrlong.punter_key
                   and wr.ca_game_type_id=wrlong.ca_game_type_id
                   and wr.ca_race_type_id=wrlong.ca_race_type_id
                   and wr.ca_program_concept_id=wrlong.ca_program_concept_id
                   and wr.ca_channel_type_id=wrlong.ca_channel_type_id
                   and wr.ca_bet_method_group_id=wrlong.ca_bet_method_group_id
                   and wr.tech_channel_id=wrlong.tech_channel_id)
                   ),
             week_act_rlong_rshort as
                   (select coalesce(wr.punter_key,wrshort.punter_key) as punter_key,
                           coalesce(wr.punter_punter_id,wrshort.punter_punter_id) as punter_punter_id,
                           coalesce(wr.ca_game_type_id,wrshort.ca_game_type_id) as ca_game_type_id,
                           coalesce(wr.ca_race_type_id,wrshort.ca_race_type_id) as ca_race_type_id,
                           coalesce(wr.ca_program_concept_id,wrshort.ca_program_concept_id) as ca_program_concept_id,
                           coalesce(wr.ca_channel_type_id,wrshort.ca_channel_type_id) as ca_channel_type_id,
                           coalesce(wr.ca_bet_method_group_id,wrshort.ca_bet_method_group_id) as ca_bet_method_group_id,
                           coalesce(wr.tech_channel_id,wrshort.tech_channel_id) as tech_channel_id,
                           coalesce(wr.revenue_wtd_plus,0) as revenue_wtd_plus,
                           coalesce(wr.revenue_wtd_rlong_minus,0) as revenue_wtd_rlong_minus,
                           coalesce(wrshort.revenue_wtd_rshort_minus,0) as revenue_wtd_rshort_minus
                      from week_act_join_long wr full outer join week_rev_minus_rshort wrshort
                     on (wr.punter_key=wrshort.punter_key
                       and wr.ca_game_type_id=wrshort.ca_game_type_id
                       and wr.ca_race_type_id=wrshort.ca_race_type_id
                       and wr.ca_program_concept_id=wrshort.ca_program_concept_id
                       and wr.ca_channel_type_id=wrshort.ca_channel_type_id
                       and wr.ca_bet_method_group_id=wrshort.ca_bet_method_group_id
                       and wr.tech_channel_id=wrshort.tech_channel_id)
            ),
            cust_agg_last_week as (
                            select  p.punter_key as lw_punter_key,
                                   cr.punter_punter_id as lw_punter_punter_id,
                                   cr.ca_game_type_id as lw_ca_game_type_id,
                                   cr.ca_race_type_id as lw_ca_race_type_id,
                                   cr.ca_program_concept_id as lw_ca_program_concept_id,
                                   cr.ca_channel_type_id as lw_ca_channel_type_id,
                                   cr.ca_bet_method_group_id as lw_ca_bet_method_group_id,
                                   cr.tech_channel_id as lw_tech_channel_id,
                                   cr.revenue_wtd as lw_revenue_wtd,
                                   cr.revenue_r13_week as lw_revenue_r13_week,
                                   cr.revenue_r52_week as lw_revenue_r52_week,
                                   cr.revenue_total as lw_revenue_total
                              from customer_revenue cr,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=cr.ca_time_day_id
                               and cr.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week=l_last_year_week),
            week_act_rlong_rshort_agg_lw as (
                            select coalesce(calw.lw_punter_key,warlrs.punter_key) as punter_key,
                                   coalesce(calw.lw_punter_punter_id,warlrs.punter_punter_id) as punter_punter_id,
                                   coalesce(calw.lw_ca_game_type_id,warlrs.ca_game_type_id) as ca_game_type_id,
                                   coalesce(calw.lw_ca_race_type_id,warlrs.ca_race_type_id) as ca_race_type_id,
                                   coalesce(calw.lw_ca_program_concept_id,warlrs.ca_program_concept_id) as ca_program_concept_id,
                                   coalesce(calw.lw_ca_channel_type_id,warlrs.ca_channel_type_id) as ca_channel_type_id,
                                   coalesce(calw.lw_ca_bet_method_group_id,warlrs.ca_bet_method_group_id) as ca_bet_method_group_id,
                                   coalesce(calw.lw_tech_channel_id,warlrs.tech_channel_id) as tech_channel_id,
                                   coalesce(warlrs.revenue_wtd_plus,0) as revenue_wtd,
                                   coalesce(calw.lw_revenue_r13_week,0)+coalesce(warlrs.revenue_wtd_plus,0)-coalesce(warlrs.revenue_wtd_rshort_minus,0) as revenue_r13,
                                   coalesce(calw.lw_revenue_r52_week,0)+coalesce(warlrs.revenue_wtd_plus,0)-coalesce(warlrs.revenue_wtd_rlong_minus,0) as revenue_r52,
                                   coalesce(calw.lw_revenue_total,0)+coalesce(warlrs.revenue_wtd_plus,0) as revenue_total
                              from cust_agg_last_week calw full outer join week_act_rlong_rshort warlrs
                               on (calw.lw_punter_key=warlrs.punter_key
                               and calw.lw_ca_game_type_id=warlrs.ca_game_type_id
                               and calw.lw_ca_race_type_id=warlrs.ca_race_type_id
                               and calw.lw_ca_program_concept_id=warlrs.ca_program_concept_id
                               and calw.lw_ca_channel_type_id=warlrs.ca_channel_type_id
                               and calw.lw_ca_bet_method_group_id=warlrs.ca_bet_method_group_id
                               and calw.lw_tech_channel_id=warlrs.tech_channel_id)
            )
            select l__act_week_id as ca_time_day_id,
                   punter_punter_id,
                   ca_game_type_id,
                   ca_race_type_id,
                   ca_program_concept_id,
                   ca_channel_type_id,
                   ca_bet_method_group_id,
                   tech_channel_id,
                   revenue_wtd,
                   revenue_r13,
                   revenue_r52,
                   revenue_total
              from week_act_rlong_rshort_agg_lw;
              
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);              
              
    end if;

 exception
    when others then
      dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
      raise;
 end load_customer_revenue;

/***************************************************************************/

procedure load_num_of_game_days(p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

  MODULE_NAME            constant varchar2(30) := 'load_num_of_game_days';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

 begin
    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from num_of_game_days
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       -- ROLLING_LONG_WEEK
       select year_week into l_r_long_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
       -- ROLLING_SHORT_WEEK
       select year_week into l_r_short_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;
       -- last year_week
       select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       dbms_output.put_line('l_r_long_first_year_week: '||l_r_long_first_year_week);
       dbms_output.put_line('l_r_short_first_year_week: '||l_r_short_first_year_week);
       dbms_output.put_line('l_last_year_week: '||l_last_year_week);
    end if;


    if l_insert then
    
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));    
    
            insert into num_of_game_days(ca_time_day_id,
                                                  punter_punter_id,
                                                  ca_game_type_id,
                                                  ca_race_type_id,
                                                  ca_program_concept_id,
                                                  ca_channel_type_id,
                                                  ca_bet_method_group_id,
                                                  tech_channel_id,
                                                  num_of_game_days_wtd,
                                                  num_of_game_days_r13_week,
                                                  num_of_game_days_r52_week,
                                                  num_of_game_days_total)
            with week_rev_act_week as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id,
                                   count(distinct ctd.full_date) as num_of_game_days_wtd_plus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_act_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id),
            week_rev_minus_rlong as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rlong_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_long_first_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id),
            week_rev_minus_rshort as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rshort_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_short_first_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id,
                                   ca.ca_race_type_id,
                                   ca.ca_program_concept_id,
                                   ca.ca_channel_type_id,
                                   ca.ca_bet_method_group_id,
                                   ca.tech_channel_id),
            week_act_join_long as (
                select coalesce(wr.punter_key,wrlong.punter_key) as punter_key,
                       coalesce(wr.punter_punter_id,wrlong.punter_punter_id) as punter_punter_id,
                       coalesce(wr.ca_game_type_id,wrlong.ca_game_type_id) as ca_game_type_id,
                       coalesce(wr.ca_race_type_id,wrlong.ca_race_type_id) as ca_race_type_id,
                       coalesce(wr.ca_program_concept_id,wrlong.ca_program_concept_id) as ca_program_concept_id,
                       coalesce(wr.ca_channel_type_id,wrlong.ca_channel_type_id) as ca_channel_type_id,
                       coalesce(wr.ca_bet_method_group_id,wrlong.ca_bet_method_group_id) as ca_bet_method_group_id,
                       coalesce(wr.tech_channel_id,wrlong.tech_channel_id) as tech_channel_id,
                       wr.num_of_game_days_wtd_plus as num_of_game_days_wtd_plus,
                       wrlong.num_game_days_wtd_rlong_minus as num_game_days_wtd_rlong_minus
                  from week_rev_act_week wr full outer join week_rev_minus_rlong wrlong
                 on (wr.punter_key=wrlong.punter_key
                   and wr.ca_game_type_id=wrlong.ca_game_type_id
                   and wr.ca_race_type_id=wrlong.ca_race_type_id
                   and wr.ca_program_concept_id=wrlong.ca_program_concept_id
                   and wr.ca_channel_type_id=wrlong.ca_channel_type_id
                   and wr.ca_bet_method_group_id=wrlong.ca_bet_method_group_id
                   and wr.tech_channel_id=wrlong.tech_channel_id)
                   ),
             week_act_rlong_rshort as
                   (select coalesce(wr.punter_key,wrshort.punter_key) as punter_key,
                           coalesce(wr.punter_punter_id,wrshort.punter_punter_id) as punter_punter_id,
                           coalesce(wr.ca_game_type_id,wrshort.ca_game_type_id) as ca_game_type_id,
                           coalesce(wr.ca_race_type_id,wrshort.ca_race_type_id) as ca_race_type_id,
                           coalesce(wr.ca_program_concept_id,wrshort.ca_program_concept_id) as ca_program_concept_id,
                           coalesce(wr.ca_channel_type_id,wrshort.ca_channel_type_id) as ca_channel_type_id,
                           coalesce(wr.ca_bet_method_group_id,wrshort.ca_bet_method_group_id) as ca_bet_method_group_id,
                           coalesce(wr.tech_channel_id,wrshort.tech_channel_id) as tech_channel_id,
                           coalesce(wr.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd_plus,
                           coalesce(wr.num_game_days_wtd_rlong_minus,0) as num_game_days_wtd_rlong_minus,
                           coalesce(wrshort.num_game_days_wtd_rshort_minus,0) as num_game_days_wtd_rshort_minus
                      from week_act_join_long wr full outer join week_rev_minus_rshort wrshort
                     on (wr.punter_key=wrshort.punter_key
                       and wr.ca_game_type_id=wrshort.ca_game_type_id
                       and wr.ca_race_type_id=wrshort.ca_race_type_id
                       and wr.ca_program_concept_id=wrshort.ca_program_concept_id
                       and wr.ca_channel_type_id=wrshort.ca_channel_type_id
                       and wr.ca_bet_method_group_id=wrshort.ca_bet_method_group_id
                       and wr.tech_channel_id=wrshort.tech_channel_id)
            ),
            cust_agg_last_week as (
                            select p.punter_key as lw_punter_key,
                                   nogd.punter_punter_id as lw_punter_punter_id,
                                   nogd.ca_game_type_id as lw_ca_game_type_id,
                                   nogd.ca_race_type_id as lw_ca_race_type_id,
                                   nogd.ca_program_concept_id as lw_ca_program_concept_id,
                                   nogd.ca_channel_type_id as lw_ca_channel_type_id,
                                   nogd.ca_bet_method_group_id as lw_ca_bet_method_group_id,
                                   nogd.tech_channel_id as lw_tech_channel_id,
                                   nogd.num_of_game_days_wtd as lw_num_of_game_days_wtd,
                                   nogd.num_of_game_days_r13_week as lw_num_of_game_days_r13_week,
                                   nogd.num_of_game_days_r52_week as lw_num_of_game_days_r52_week,
                                   nogd.num_of_game_days_total as lw_num_of_game_days_total
                              from num_of_game_days nogd,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=nogd.ca_time_day_id
                               and nogd.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week=l_last_year_week),
            week_act_rlong_rshort_agg_lw as (
                            select coalesce(calw.lw_punter_key,warlrs.punter_key) as punter_key,
                                   coalesce(warlrs.punter_punter_id, calw.lw_punter_punter_id) as punter_punter_id,
                                   coalesce(calw.lw_ca_game_type_id,warlrs.ca_game_type_id) as ca_game_type_id,
                                   coalesce(calw.lw_ca_race_type_id,warlrs.ca_race_type_id) as ca_race_type_id,
                                   coalesce(calw.lw_ca_program_concept_id,warlrs.ca_program_concept_id) as ca_program_concept_id,
                                   coalesce(calw.lw_ca_channel_type_id,warlrs.ca_channel_type_id) as ca_channel_type_id,
                                   coalesce(calw.lw_ca_bet_method_group_id,warlrs.ca_bet_method_group_id) as ca_bet_method_group_id,
                                   coalesce(calw.lw_tech_channel_id,warlrs.tech_channel_id) as tech_channel_id,
                                   coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd,
                                   coalesce(calw.lw_num_of_game_days_r13_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rshort_minus,0) as num_of_game_days_r13,
                                   coalesce(calw.lw_num_of_game_days_r52_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rlong_minus,0) as num_of_game_days_r52,
                                   coalesce(calw.lw_num_of_game_days_total,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_total
                              from cust_agg_last_week calw full outer join week_act_rlong_rshort warlrs
                               on (calw.lw_punter_key=warlrs.punter_key
                               and calw.lw_ca_game_type_id=warlrs.ca_game_type_id
                               and calw.lw_ca_race_type_id=warlrs.ca_race_type_id
                               and calw.lw_ca_program_concept_id=warlrs.ca_program_concept_id
                               and calw.lw_ca_channel_type_id=warlrs.ca_channel_type_id
                               and calw.lw_ca_bet_method_group_id=warlrs.ca_bet_method_group_id
                               and calw.lw_tech_channel_id=warlrs.tech_channel_id)
            )
            select l__act_week_id as ca_time_day_id,
                   punter_punter_id,
                   ca_game_type_id,
                   ca_race_type_id,
                   ca_program_concept_id,
                   ca_channel_type_id,
                   ca_bet_method_group_id,
                   tech_channel_id,
                   num_of_game_days_wtd,
                   num_of_game_days_r13,
                   num_of_game_days_r52,
                   num_of_game_days_total
              from week_act_rlong_rshort_agg_lw;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_num_of_game_days;
------------------------------------------------------------------------------
 procedure load_num_of_game_days_total(p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

  MODULE_NAME            constant varchar2(30) := 'load_num_of_game_days_total';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
 begin

    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from num_of_game_days_total
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       -- ROLLING_LONG_WEEK
       select year_week into l_r_long_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
       -- ROLLING_SHORT_WEEK
       select year_week into l_r_short_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;
       -- last year_week
       select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       dbms_output.put_line('l_r_long_first_year_week: '||l_r_long_first_year_week);
       dbms_output.put_line('l_r_short_first_year_week: '||l_r_short_first_year_week);
       dbms_output.put_line('l_last_year_week: '||l_last_year_week);
    end if;

    if l_insert then
    
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));    
    
            insert into num_of_game_days_total (ca_time_day_id,
                            punter_punter_id,
                            num_of_game_days_wtd,
                            num_of_game_days_r13_week,
                            num_of_game_days_r52_week,
                            num_of_game_days_total)
            with week_gdays_act_week as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   count(distinct ctd.full_date) as game_days_wtd_plus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                              -- and ctd.full_date between to_date('20161226','yyyymmdd') and to_date('20170101','yyyymmdd') -- Senaste veckan / Ny vecka i 52-veckors och 13-veckors
                               and ctd.year_week = l_act_year_week
                             group by p.punter_key),
            week_gdays_minus_rlong as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   count(distinct ctd.full_date) as game_days_wtd_rlong_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                              -- and ctd.full_date between to_date('20160104','yyyymmdd') and to_date('20160110','yyyymmdd') -- Första veckan i föregående 52-veckor period
                              and ctd.year_week = l_r_long_first_year_week
                             group by p.punter_key),
            week_gdays_minus_rshort as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   count(distinct ctd.full_date) as game_days_wtd_rshort_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                              -- and ctd.full_date between to_date('20161003','yyyymmdd') and to_date('20161009','yyyymmdd') -- Första veckan i föregående 13-veckors period
                               and ctd.year_week = l_r_short_first_year_week
                             group by p.punter_key),
            week_act_join_long as (
                select coalesce(wr.punter_key,wrlong.punter_key) as punter_key,
                       coalesce(wr.punter_punter_id,wrlong.punter_punter_id) as punter_punter_id,
                       wr.game_days_wtd_plus as game_days_wtd_plus,
                       wrlong.game_days_wtd_rlong_minus as game_days_wtd_rlong_minus
                  from week_gdays_act_week wr full outer join week_gdays_minus_rlong wrlong
                 on (wr.punter_key=wrlong.punter_key)
                   ),
             week_act_rlong_rshort as
                   (select coalesce(wr.punter_key,wrshort.punter_key) as punter_key,
                           coalesce(wr.punter_punter_id,wrshort.punter_punter_id) as punter_punter_id,
                           coalesce(wr.game_days_wtd_plus,0) as game_days_wtd_plus,
                           coalesce(wr.game_days_wtd_rlong_minus,0) as game_days_wtd_rlong_minus,
                           coalesce(wrshort.game_days_wtd_rshort_minus,0) as game_days_wtd_rshort_minus
                      from week_act_join_long wr full outer join week_gdays_minus_rshort wrshort
                     on (wr.punter_key=wrshort.punter_key)
            ),
            cust_agg_last_week as (
                            select p.punter_key as lw_punter_key,
                                   nogdt.punter_punter_id as lw_punter_punter_id,
                                   nogdt.num_of_game_days_wtd as lw_num_of_game_days_wtd,
                                   nogdt.num_of_game_days_r13_week as lw_num_of_game_days_r13_week,
                                   nogdt.num_of_game_days_r52_week as lw_num_of_game_days_r52_week,
                                   nogdt.num_of_game_days_total as lw_num_of_game_days_total
                              from num_of_game_days_total nogdt,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=nogdt.ca_time_day_id
                               and nogdt.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week=l_last_year_week),
            week_act_rlong_rshort_agg_lw as (
                            select coalesce(calw.lw_punter_key,warlrs.punter_key) as punter_key,
                                   coalesce(warlrs.punter_punter_id, calw.lw_punter_punter_id) as punter_punter_id,
                                   coalesce(warlrs.game_days_wtd_plus,0) as game_days_wtd,
                                   coalesce(calw.lw_num_of_game_days_r13_week,0)+coalesce(warlrs.game_days_wtd_plus,0)-coalesce(warlrs.game_days_wtd_rshort_minus,0) as game_days_r13,
                                   coalesce(calw.lw_num_of_game_days_r52_week,0)+coalesce(warlrs.game_days_wtd_plus,0)-coalesce(warlrs.game_days_wtd_rlong_minus,0) as game_days_r52,
                                   coalesce(calw.lw_num_of_game_days_total,0)+coalesce(warlrs.game_days_wtd_plus,0) as game_days_total
                              from cust_agg_last_week calw full outer join week_act_rlong_rshort warlrs
                               on (calw.lw_punter_key=warlrs.punter_key)
            )
            select l__act_week_id as ca_time_day_id,
                   punter_punter_id,
                   game_days_wtd,
                   game_days_r13,
                   game_days_r52,
                   game_days_total
              from week_act_rlong_rshort_agg_lw;
              
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);              
              
    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_num_of_game_days_total;
-------------------------------------------------------------------------------
procedure load_nogd_trot_concept(p_date date) is

   last_day_of_week_indicator pls_integer;
   this_week_last_day_id      pls_integer;
   row_count                  pls_integer;

   this_week                  pls_integer;
   last_week                  pls_integer;
   r13_week                   pls_integer;
   r52_week                   pls_integer;


  MODULE_NAME            constant varchar2(30) := 'load_nogd_trot_concept';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from num_of_game_days_trot_concept
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

  select year_week into last_week from ca_time_day
  where full_date = p_date - SEVEN;

  select year_week into r13_week from ca_time_day
  where full_date = p_date - (SEVEN * ROLLING_SHORT_WEEK);

  select year_week into r52_week  from ca_time_day
  where full_date = p_date - (SEVEN * ROLLING_LONG_WEEK);

  insert into num_of_game_days_trot_concept(
      ca_time_day_id,
      punter_punter_id,
      ca_program_concept_id,
      num_of_game_days_wtd,
      num_of_game_days_r13_week,
      num_of_game_days_r52_week,
      num_of_game_days_total)
  with

  sub1 as(
    select
      punter_key,
      max(punter_id)                  as punter_punter_id,
      ca_program_concept_id,
      count(distinct full_date)      as num_of_game_days
    from customer_activity,
          ca_time_day,
          punter
    where customer_activity.ca_time_day_id = ca_time_day.id
      and customer_activity.punter_punter_id = punter.punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_race_type_id = TROT_ID
      and ca_time_day.year_week = this_week
    group by
      punter_key,
      ca_program_concept_id),

  sub2 as(
    select
      punter_key,
      max(punter_id)                  as punter_punter_id,
      ca_program_concept_id,
      count(distinct full_date)      as num_of_game_days
    from customer_activity,
          ca_time_day,
          punter
    where customer_activity.ca_time_day_id = ca_time_day.id
      and customer_activity.punter_punter_id = punter.punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_race_type_id = TROT_ID
      and ca_time_day.year_week = r52_week
    group by
      punter_key,
      ca_program_concept_id),

  sub3 as(
    select
      punter_key,
      max(punter_id)                  as punter_punter_id,
      ca_program_concept_id,
      count(distinct full_date)      as num_of_game_days
    from customer_activity,
          ca_time_day,
          punter
    where customer_activity.ca_time_day_id = ca_time_day.id
      and customer_activity.punter_punter_id = punter.punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_race_type_id = TROT_ID
      and ca_time_day.year_week = r13_week
    group by
      punter_key,
      ca_program_concept_id),

  sub4 as(
    select
      coalesce(sub1.punter_key, sub2.punter_key)                       as punter_key,
      coalesce(sub1.punter_punter_id, sub2.punter_punter_id)           as punter_punter_id,
      coalesce(sub1.ca_program_concept_id, sub2.ca_program_concept_id) as ca_program_concept_id,
      coalesce(sub1.num_of_game_days, ZERO)                            as num_of_game_days,
      coalesce(sub2.num_of_game_days, ZERO)                            as num_of_game_days_r52_week
    from sub1 full outer join sub2
      on sub1.punter_key = sub2.punter_key and
         sub1.ca_program_concept_id = sub2.ca_program_concept_id),

  sub5 as(
    select coalesce(sub4.punter_key, sub3.punter_key)                 as punter_key,
      coalesce(sub4.punter_punter_id, sub3.punter_punter_id)           as punter_punter_id,
      coalesce(sub4.ca_program_concept_id, sub3.ca_program_concept_id) as ca_program_concept_id,
      coalesce(sub4.num_of_game_days, ZERO)                            as num_of_game_days,
      coalesce(sub4.num_of_game_days_r52_week, ZERO)                   as num_of_game_days_r52_week,
      coalesce(sub3.num_of_game_days, ZERO)                            as num_of_game_days_r13_week
    from sub3 full outer join sub4
      on sub3.punter_key = sub4.punter_key and
         sub3.ca_program_concept_id = sub4.ca_program_concept_id),

  sub6 as(
    select
      punter_key                                 as punter_key,
      punter_punter_id                           as punter_punter_id,
      ca_program_concept_id                      as ca_program_concept_id,
      num_of_game_days_r13_week                  as num_of_game_days_r13_week,
      num_of_game_days_r52_week                  as num_of_game_days_r52_week,
      num_of_game_days_total                     as num_of_game_days_total
    from num_of_game_days_trot_concept,
          ca_time_day,
          punter
    where ca_time_day_id = ca_time_day.id
      and punter_punter_id = punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_time_day.year_week = last_week),

  sub7 as(
    select
      coalesce(sub6.punter_punter_id, sub5.punter_punter_id)  as punter_punter_id,
      coalesce(sub6.ca_program_concept_id, sub5.ca_program_concept_id)
                                                               as ca_program_concept_id,

      coalesce(sub5.num_of_game_days, ZERO)                   as num_of_game_days_wtd,

      coalesce(sub6.num_of_game_days_r13_week, ZERO) + coalesce(sub5.num_of_game_days, ZERO) -
        coalesce(sub5.num_of_game_days_r13_week, ZERO)        as num_of_game_days_r13,

      coalesce(sub6.num_of_game_days_r52_week, ZERO) + coalesce(sub5.num_of_game_days, ZERO) -
        coalesce(sub5.num_of_game_days_r52_week, ZERO)        as num_of_game_days_r52,

      coalesce(sub6.num_of_game_days_total, ZERO) + coalesce(sub5.num_of_game_days, ZERO)
                                                               as num_of_game_days_total
    from sub5 full outer join sub6
    on  sub5.punter_key = sub6.punter_key and
        sub5.ca_program_concept_id = sub6.ca_program_concept_id)

    select
      this_week_last_day_id,
      punter_punter_id,
      ca_program_concept_id,
      num_of_game_days_wtd,
      num_of_game_days_r13,
      num_of_game_days_r52,
      num_of_game_days_total
    from sub7;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_nogd_trot_concept;


------------------------------------------------------------------------------

procedure load_nogd_race_type (p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

  MODULE_NAME            constant varchar2(30) := 'load_nogd_race_type';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

 begin
    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from num_of_game_days_race_type
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       -- ROLLING_LONG_WEEK
       select year_week into l_r_long_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
       -- ROLLING_SHORT_WEEK
       select year_week into l_r_short_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;
       -- last year_week
       select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       dbms_output.put_line('l_r_long_first_year_week: '||l_r_long_first_year_week);
       dbms_output.put_line('l_r_short_first_year_week: '||l_r_short_first_year_week);
       dbms_output.put_line('l_last_year_week: '||l_last_year_week);
    end if;


    if l_insert then

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));    
    
        insert into num_of_game_days_race_type(
                                               ca_time_day_id,
                                               punter_punter_id,
                                               ca_race_type_id,
                                               num_of_game_days_wtd,
                                               num_of_game_days_r13_week,
                                               num_of_game_days_r52_week,
                                               num_of_game_days_total
                                              )
            with week_rev_act_week as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_race_type_id,
                                   count(distinct ctd.full_date) as num_of_game_days_wtd_plus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_act_year_week
                             group by p.punter_key,
                                   ca.ca_race_type_id),
            week_rev_minus_rlong as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_race_type_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rlong_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_long_first_year_week
                             group by p.punter_key,
                                   ca.ca_race_type_id),
            week_rev_minus_rshort as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_race_type_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rshort_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_short_first_year_week
                             group by p.punter_key,
                                   ca.ca_race_type_id),
            week_act_join_long as (
                select coalesce(wr.punter_key,wrlong.punter_key) as punter_key,
                       coalesce(wr.punter_punter_id,wrlong.punter_punter_id) as punter_punter_id,
                       coalesce(wr.ca_race_type_id,wrlong.ca_race_type_id) as ca_race_type_id,
                       wr.num_of_game_days_wtd_plus as num_of_game_days_wtd_plus,
                       wrlong.num_game_days_wtd_rlong_minus as num_game_days_wtd_rlong_minus
                  from week_rev_act_week wr full outer join week_rev_minus_rlong wrlong
                 on (wr.punter_key=wrlong.punter_key
                   and wr.ca_race_type_id=wrlong.ca_race_type_id)
                   ),
             week_act_rlong_rshort as
                   (select coalesce(wr.punter_key,wrshort.punter_key) as punter_key,
                           coalesce(wr.punter_punter_id,wrshort.punter_punter_id) as punter_punter_id,
                           coalesce(wr.ca_race_type_id,wrshort.ca_race_type_id) as ca_race_type_id,
                           coalesce(wr.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd_plus,
                           coalesce(wr.num_game_days_wtd_rlong_minus,0) as num_game_days_wtd_rlong_minus,
                           coalesce(wrshort.num_game_days_wtd_rshort_minus,0) as num_game_days_wtd_rshort_minus
                      from week_act_join_long wr full outer join week_rev_minus_rshort wrshort
                     on (wr.punter_key=wrshort.punter_key
                       and wr.ca_race_type_id=wrshort.ca_race_type_id)
            ),
            cust_agg_last_week as (
                            select p.punter_key as lw_punter_key,
                                   nogd.punter_punter_id as lw_punter_punter_id,
                                   nogd.ca_race_type_id as lw_ca_race_type_id,
                                   nogd.num_of_game_days_wtd as lw_num_of_game_days_wtd,
                                   nogd.num_of_game_days_r13_week as lw_num_of_game_days_r13_week,
                                   nogd.num_of_game_days_r52_week as lw_num_of_game_days_r52_week,
                                   nogd.num_of_game_days_total as lw_num_of_game_days_total
                              from num_of_game_days_race_type nogd,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=nogd.ca_time_day_id
                               and nogd.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week=l_last_year_week),
            week_act_rlong_rshort_agg_lw as (
                            select coalesce(calw.lw_punter_key,warlrs.punter_key) as punter_key,
                                   coalesce(calw.lw_punter_punter_id,warlrs.punter_punter_id) as punter_punter_id,
                                   coalesce(calw.lw_ca_race_type_id,warlrs.ca_race_type_id) as ca_race_type_id,
                                   coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd,
                                   coalesce(calw.lw_num_of_game_days_r13_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rshort_minus,0) as num_of_game_days_r13,
                                   coalesce(calw.lw_num_of_game_days_r52_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rlong_minus,0) as num_of_game_days_r52,
                                   coalesce(calw.lw_num_of_game_days_total,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_total
                              from cust_agg_last_week calw full outer join week_act_rlong_rshort warlrs
                               on (calw.lw_punter_key=warlrs.punter_key
                               and calw.lw_ca_race_type_id=warlrs.ca_race_type_id)
            )
            select l__act_week_id as ca_time_day_id,
                   punter_punter_id,
                   ca_race_type_id,
                   num_of_game_days_wtd,
                   num_of_game_days_r13,
                   num_of_game_days_r52,
                   num_of_game_days_total
              from week_act_rlong_rshort_agg_lw;
              
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);              
              
    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_nogd_race_type;

------------------------------------------------------------------------------

procedure load_nogd_hr_race_type (p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

  MODULE_NAME            constant varchar2(30) := 'load_nogd_hr_race_type';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

 begin
    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from num_of_game_days_hr_race_type
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       -- ROLLING_LONG_WEEK
       select year_week into l_r_long_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
       -- ROLLING_SHORT_WEEK
       select year_week into l_r_short_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;
       -- last year_week
       select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       dbms_output.put_line('l_r_long_first_year_week: '||l_r_long_first_year_week);
       dbms_output.put_line('l_r_short_first_year_week: '||l_r_short_first_year_week);
       dbms_output.put_line('l_last_year_week: '||l_last_year_week);
    end if;


    if l_insert then
    
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));    
    
        insert into num_of_game_days_hr_race_type(
                                               ca_time_day_id,
                                               punter_punter_id,
                                               ca_race_type_id,
                                               num_of_game_days_wtd,
                                               num_of_game_days_r13_week,
                                               num_of_game_days_r52_week,
                                               num_of_game_days_total
                                              )
            with week_rev_act_week as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_race_type_id,
                                   count(distinct ctd.full_date) as num_of_game_days_wtd_plus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_act_year_week
                               and ca.ca_game_type_id = HORSE_RACING_ID
                             group by p.punter_key,
                                   ca.ca_race_type_id),
            week_rev_minus_rlong as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_race_type_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rlong_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_long_first_year_week
                               and ca.ca_game_type_id = HORSE_RACING_ID
                             group by p.punter_key,
                                   ca.ca_race_type_id),
            week_rev_minus_rshort as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_race_type_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rshort_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_short_first_year_week
                               and ca.ca_game_type_id = HORSE_RACING_ID
                             group by p.punter_key,
                                   ca.ca_race_type_id),
            week_act_join_long as (
                select coalesce(wr.punter_key,wrlong.punter_key) as punter_key,
                       coalesce(wr.punter_punter_id,wrlong.punter_punter_id) as punter_punter_id,
                       coalesce(wr.ca_race_type_id,wrlong.ca_race_type_id) as ca_race_type_id,
                       wr.num_of_game_days_wtd_plus as num_of_game_days_wtd_plus,
                       wrlong.num_game_days_wtd_rlong_minus as num_game_days_wtd_rlong_minus
                  from week_rev_act_week wr full outer join week_rev_minus_rlong wrlong
                 on (wr.punter_key=wrlong.punter_key
                   and wr.ca_race_type_id=wrlong.ca_race_type_id)
                   ),
             week_act_rlong_rshort as
                   (select coalesce(wr.punter_key,wrshort.punter_key) as punter_key,
                           coalesce(wr.punter_punter_id,wrshort.punter_punter_id) as punter_punter_id,
                           coalesce(wr.ca_race_type_id,wrshort.ca_race_type_id) as ca_race_type_id,
                           coalesce(wr.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd_plus,
                           coalesce(wr.num_game_days_wtd_rlong_minus,0) as num_game_days_wtd_rlong_minus,
                           coalesce(wrshort.num_game_days_wtd_rshort_minus,0) as num_game_days_wtd_rshort_minus
                      from week_act_join_long wr full outer join week_rev_minus_rshort wrshort
                     on (wr.punter_key=wrshort.punter_key
                       and wr.ca_race_type_id=wrshort.ca_race_type_id)
            ),
            cust_agg_last_week as (
                            select p.punter_key as lw_punter_key,
                                   nogd.punter_punter_id as lw_punter_punter_id,
                                   nogd.ca_race_type_id as lw_ca_race_type_id,
                                   nogd.num_of_game_days_wtd as lw_num_of_game_days_wtd,
                                   nogd.num_of_game_days_r13_week as lw_num_of_game_days_r13_week,
                                   nogd.num_of_game_days_r52_week as lw_num_of_game_days_r52_week,
                                   nogd.num_of_game_days_total as lw_num_of_game_days_total
                              from num_of_game_days_hr_race_type nogd,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=nogd.ca_time_day_id
                               and nogd.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week=l_last_year_week),
            week_act_rlong_rshort_agg_lw as (
                            select coalesce(calw.lw_punter_key,warlrs.punter_key) as punter_key,
                                   coalesce(calw.lw_punter_punter_id,warlrs.punter_punter_id) as punter_punter_id,
                                   coalesce(calw.lw_ca_race_type_id,warlrs.ca_race_type_id) as ca_race_type_id,
                                   coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd,
                                   coalesce(calw.lw_num_of_game_days_r13_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rshort_minus,0) as num_of_game_days_r13,
                                   coalesce(calw.lw_num_of_game_days_r52_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rlong_minus,0) as num_of_game_days_r52,
                                   coalesce(calw.lw_num_of_game_days_total,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_total
                              from cust_agg_last_week calw full outer join week_act_rlong_rshort warlrs
                               on (calw.lw_punter_key=warlrs.punter_key
                               and calw.lw_ca_race_type_id=warlrs.ca_race_type_id)
            )
            select l__act_week_id as ca_time_day_id,
                   punter_punter_id,
                   ca_race_type_id,
                   num_of_game_days_wtd,
                   num_of_game_days_r13,
                   num_of_game_days_r52,
                   num_of_game_days_total
              from week_act_rlong_rshort_agg_lw;
              
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);              
              
    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_nogd_hr_race_type;

------------------------------------------------------------------------------

procedure load_nogd_game_type (p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

  MODULE_NAME            constant varchar2(30) := 'load_nogd_game_type';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

 begin
    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from num_of_game_days_game_type
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       -- ROLLING_LONG_WEEK
       select year_week into l_r_long_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
       -- ROLLING_SHORT_WEEK
       select year_week into l_r_short_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;
       -- last year_week
       select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       dbms_output.put_line('l_r_long_first_year_week: '||l_r_long_first_year_week);
       dbms_output.put_line('l_r_short_first_year_week: '||l_r_short_first_year_week);
       dbms_output.put_line('l_last_year_week: '||l_last_year_week);
    end if;


    if l_insert then

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));    
    
        insert into num_of_game_days_game_type(
                                               ca_time_day_id,
                                               punter_punter_id,
                                               ca_game_type_id,
                                               num_of_game_days_wtd,
                                               num_of_game_days_r13_week,
                                               num_of_game_days_r52_week,
                                               num_of_game_days_total
                                              )
            with week_rev_act_week as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   count(distinct ctd.full_date) as num_of_game_days_wtd_plus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_act_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id),
            week_rev_minus_rlong as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rlong_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_long_first_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id),
            week_rev_minus_rshort as (
                            select p.punter_key,
                                   max(p.punter_id) punter_punter_id,
                                   ca.ca_game_type_id,
                                   count(distinct ctd.full_date) as num_game_days_wtd_rshort_minus
                              from customer_activity ca,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=ca.ca_time_day_id
                               and ca.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week = l_r_short_first_year_week
                             group by p.punter_key,
                                   ca.ca_game_type_id),
            week_act_join_long as (
                select coalesce(wr.punter_key,wrlong.punter_key) as punter_key,
                       coalesce(wr.punter_punter_id,wrlong.punter_punter_id) as punter_punter_id,
                       coalesce(wr.ca_game_type_id,wrlong.ca_game_type_id) as ca_game_type_id,
                       wr.num_of_game_days_wtd_plus as num_of_game_days_wtd_plus,
                       wrlong.num_game_days_wtd_rlong_minus as num_game_days_wtd_rlong_minus
                  from week_rev_act_week wr full outer join week_rev_minus_rlong wrlong
                 on (wr.punter_key=wrlong.punter_key
                   and wr.ca_game_type_id=wrlong.ca_game_type_id)
                   ),
             week_act_rlong_rshort as
                   (select coalesce(wr.punter_key,wrshort.punter_key) as punter_key,
                           coalesce(wr.punter_punter_id,wrshort.punter_punter_id) as punter_punter_id,
                           coalesce(wr.ca_game_type_id,wrshort.ca_game_type_id) as ca_game_type_id,
                           coalesce(wr.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd_plus,
                           coalesce(wr.num_game_days_wtd_rlong_minus,0) as num_game_days_wtd_rlong_minus,
                           coalesce(wrshort.num_game_days_wtd_rshort_minus,0) as num_game_days_wtd_rshort_minus
                      from week_act_join_long wr full outer join week_rev_minus_rshort wrshort
                     on (wr.punter_key=wrshort.punter_key
                       and wr.ca_game_type_id=wrshort.ca_game_type_id)
            ),
            cust_agg_last_week as (
                            select p.punter_key as lw_punter_key,
                                   nogd.punter_punter_id as lw_punter_punter_id,
                                   nogd.ca_game_type_id as lw_ca_game_type_id,
                                   nogd.num_of_game_days_wtd as lw_num_of_game_days_wtd,
                                   nogd.num_of_game_days_r13_week as lw_num_of_game_days_r13_week,
                                   nogd.num_of_game_days_r52_week as lw_num_of_game_days_r52_week,
                                   nogd.num_of_game_days_total as lw_num_of_game_days_total
                              from num_of_game_days_game_type nogd,
                                   ca_time_day ctd,
                                   punter p
                             where ctd.id=nogd.ca_time_day_id
                               and nogd.punter_punter_id=p.punter_id
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               and ctd.year_week=l_last_year_week),
            week_act_rlong_rshort_agg_lw as (
                            select coalesce(calw.lw_punter_key,warlrs.punter_key) as punter_key,
                                   coalesce(calw.lw_punter_punter_id,warlrs.punter_punter_id) as punter_punter_id,
                                   coalesce(calw.lw_ca_game_type_id,warlrs.ca_game_type_id) as ca_game_type_id,
                                   coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_wtd,
                                   coalesce(calw.lw_num_of_game_days_r13_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rshort_minus,0) as num_of_game_days_r13,
                                   coalesce(calw.lw_num_of_game_days_r52_week,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0)-coalesce(warlrs.num_game_days_wtd_rlong_minus,0) as num_of_game_days_r52,
                                   coalesce(calw.lw_num_of_game_days_total,0)+coalesce(warlrs.num_of_game_days_wtd_plus,0) as num_of_game_days_total
                              from cust_agg_last_week calw full outer join week_act_rlong_rshort warlrs
                               on (calw.lw_punter_key=warlrs.punter_key
                               and calw.lw_ca_game_type_id=warlrs.ca_game_type_id)
            )
            select l__act_week_id as ca_time_day_id,
                   punter_punter_id,
                   ca_game_type_id,
                   num_of_game_days_wtd,
                   num_of_game_days_r13,
                   num_of_game_days_r52,
                   num_of_game_days_total
              from week_act_rlong_rshort_agg_lw;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);
              
              
    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_nogd_game_type;



-----------------------------------------------------------------------

------------------------------------------------------------------------------

procedure load_ltgd_together (p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

  MODULE_NAME            constant varchar2(30) := 'load_ltgd_together';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

 begin
    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from latest_team_game_date_together
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       -- last year_week
       begin
          select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       exception
          when no_data_found then l_last_year_week:=null;
       end;
    end if;


    if l_insert then
        ld_start_time:=sysdate;
        
      
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => null,
                                 pio_id => ln_batch_log_id,
                                 pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));
    
    
            insert into latest_team_game_date_together(ca_time_day_id, team_account_index, latest_game_date)
            with act_week as
                   (select team_account_index, max(full_date) f_date
                      from customer_together_activity cta, ca_time_day ctd
                     where ctd.id=cta.ca_time_day_id
                       and year_week=l_act_year_week
                     group by team_account_index),
                 last_week as
                   (select team_account_index, latest_game_date
                      from latest_team_game_date_together cta, ca_time_day ctd
                     where ctd.id=cta.ca_time_day_id
                       and year_week=l_last_year_week)
              select l__act_week_id,
                     coalesce(aw.team_account_index, lw.team_account_index) as team_account_index,
                     coalesce(aw.f_date, lw.latest_game_date) as latest_game_date
                from act_week aw full outer join last_week lw
                  on (aw.team_account_index=lw.team_account_index);
                  
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);                  
                  
    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_ltgd_together;
------------------------------------------------------------------------------

procedure load_nogd_together (p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;

  MODULE_NAME            constant varchar2(30) := 'load_nogd_together';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

 begin

    dbms_output.put_line('början');

    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from num_of_game_days_together
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First week of the last ROLLING_LONG_WEEK and ROLLING_SHORT_WEEK year week number
    if l_insert then
       dbms_output.put_line('insert 1');
       -- ROLLING_LONG_WEEK
       begin
          select year_week into l_r_long_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
       exception
          when no_data_found then l_r_long_first_year_week:=null;
       end;
       dbms_output.put_line('insert 2');
       -- ROLLING_SHORT_WEEK
       begin
          select year_week into l_r_short_first_year_week from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;
       exception
          when no_data_found then l_r_short_first_year_week:=null;
       end;
       dbms_output.put_line('insert 3');
       -- last year_week
       begin
         select year_week into l_last_year_week from ca_time_day a where full_date=p_date-7;
       exception
         when no_data_found then l_last_year_week:=null;
       end;

       dbms_output.put_line('l_r_long_first_year_week: '||l_r_long_first_year_week);
       dbms_output.put_line('l_r_short_first_year_week: '||l_r_short_first_year_week);
       dbms_output.put_line('l_last_year_week: '||l_last_year_week);
    end if;


    if l_insert then

        ld_start_time:=sysdate;
        
      
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => null,
                                 pio_id => ln_batch_log_id,
                                 pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));


       ------------ 1-- INSERT I TEMPTABELL: NOGD_ACT_WEEK_TOGETHER_TEMP --------------

        insert into nogd_act_week_together_temp(ca_time_day_id,
                                                member_account_index,
                                                nogd_member_act_week,
                                                nogd_player_act_week,
                                                nogd_captain_act_week,
                                                nogd_team_agg_act_week,
                                                num_teams_act_week,
                                                num_captain_teams_act_week)
            with player_act_week as
                       (select player_account_index,
                               count(distinct ca_time_day_id||':'||team_account_index) nogd_player_act_week
                          from spst.customer_together_activity cta, spst.ca_time_day ctd
                         where ctd.id=cta.ca_time_day_id
                           --and ctd.year_week between p_start_week AND p_end_week
                           and ctd.year_week = l_act_year_week
                         group by player_account_index),
                 captain_act_week as
                       (select captain_account_index,
                               count(distinct ca_time_day_id||':'||team_account_index) nogd_captain_act_week,
                               count(distinct team_account_index) num_captain_teams_act_week
                          from spst.customer_together_activity cta, spst.ca_time_day ctd
                         where ctd.id=cta.ca_time_day_id
                           --and ctd.year_week between p_start_week AND p_end_week
                           and ctd.year_week = l_act_year_week
                         group by captain_account_index),
                 member_act_week as
                       (select member_account_index,
                               count(distinct ca_time_day_id||':'||team_account_index) nogd_member_act_week
                          from spst.customer_together_activity cta, spst.ca_time_day ctd
                         where ctd.id=cta.ca_time_day_id
                           --and ctd.year_week between p_start_week AND p_end_week
                           and ctd.year_week = l_act_year_week
                         group by member_account_index),
                  team_num_of_days as
                       (select team_account_index,
                               count(distinct ca_time_day_id||':'||team_account_index) nogd_team_agg_act_week,
                               1 as num_teams_act_week
                          from spst.customer_together_activity cta, spst.ca_time_day ctd
                         where ctd.id=cta.ca_time_day_id
                           --and ctd.year_week between p_start_week AND p_end_week
                           and ctd.year_week = l_act_year_week
                         group by team_account_index),
                  team_member as
                       (select konto_kontoindex, tillsammans_lag_kontoindex
                          from mdb.lagmedlemmar
                         where slutdatum is null or slutdatum > p_date)
                select l__act_week_id as ca_time_day_id,
                       maw.member_account_index,
                       maw.nogd_member_act_week,
                       coalesce(paw.nogd_player_act_week,0) as nogd_player_act_week,
                       coalesce(caw.nogd_captain_act_week,0) as nogd_captain_act_week,
                       sum(tnod.nogd_team_agg_act_week) nogd_team_agg_act_week,
                       sum(tnod.num_teams_act_week) num_teams_act_week,
                       coalesce(caw.num_captain_teams_act_week,0) as num_captain_teams_act_week
                from member_act_week maw full outer join player_act_week paw
                  on (maw.member_account_index=paw.player_account_index)
                                    full outer join captain_act_week caw
                  on (maw.member_account_index=caw.captain_account_index)
                                    inner join team_member tm
                  on (maw.member_account_index=tm.konto_kontoindex)
                                    inner join team_num_of_days tnod
                  on (tm.tillsammans_lag_kontoindex=tnod.team_account_index)
                group by maw.member_account_index,
                         maw.nogd_member_act_week,
                         paw.nogd_player_act_week,
                         caw.nogd_captain_act_week,
                         caw.num_captain_teams_act_week;


            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

            ln_batch_log_id:=null;
            
----------------2. INSERT I NUM_OF_GAME_DAYS_TOGETHER --------------------------

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 2,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);


       insert into  num_of_game_days_together (ca_time_day_id,
                                              punter_punter_id,
                                              nogd_member_act_week,
                                              nogd_member_r13_week,
                                              nogd_member_r52_week,
                                              nogd_member_total,
                                              nogd_player_act_week,
                                              nogd_player_r13_week,
                                              nogd_player_r52_week,
                                              nogd_player_total,
                                              nogd_captain_act_week,
                                              nogd_captain_r13_week,
                                              nogd_captain_r52_week,
                                              nogd_captain_total,
                                              nogd_team_agg_act_week,
                                              nogd_team_agg_r13_week,
                                              nogd_team_agg_r52_week,
                                              nogd_team_agg_total,
                                              num_teams_act_week,
                                              num_teams_r13_week,
                                              num_teams_r52_week,
                                              num_teams_total,
                                              num_captain_teams_act_week,
                                              num_captain_teams_r13_week,
                                              num_captain_teams_r52_week,
                                              num_captain_teams_total)

            with act_week as -- Aktuell vecka
                           (select pc.punter_key,
                                   pc.punter_id,
                                   nogdt.member_account_index,
                                   nogdt.nogd_member_act_week,
                                   nogdt.nogd_player_act_week,
                                   nogdt.nogd_captain_act_week,
                                   nogdt.nogd_team_agg_act_week,
                                   nogdt.num_teams_act_week,
                                   nogdt.num_captain_teams_act_week
                              from nogd_act_week_together_temp nogdt,
                                   punter_current pc
                             where nogdt.member_account_index=pc.acc_index
                               and pc.kontotyp=TYPE_OF_ACCOUNT
                               ),
                week_short as -- 13 VECKOR SEDAN
                            (select p.punter_key,
                                   p.punter_id,
                                   nogdt.nogd_member_act_week as nogd_member_act_week_short,
                                   nogdt.nogd_player_act_week as nogd_player_act_week_short,
                                   nogdt.nogd_captain_act_week as nogd_captain_act_week_short,
                                   nogdt.nogd_team_agg_act_week as nogd_team_agg_act_week_short,
                                   nogdt.num_teams_act_week as num_teams_act_week_short,
                                   nogdt.num_captain_teams_act_week as num_cap_teams_act_week_short
                              from num_of_game_days_together nogdt,
                                   punter p,
                                   ca_time_day ctd
                             where nogdt.punter_punter_id=p.punter_id
                               and nogdt.ca_time_day_id=ctd.id
                               and ctd.year_week=l_r_short_first_year_week
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               ),
                week_long as -- 52 VECKOR SEDAN
                            (select p.punter_key,
                                   p.punter_id,
                                   nogdt.nogd_member_act_week as nogd_member_act_week_long,
                                   nogdt.nogd_player_act_week as nogd_player_act_week_long,
                                   nogdt.nogd_captain_act_week as nogd_captain_act_week_long,
                                   nogdt.nogd_team_agg_act_week as nogd_team_agg_act_week_long,
                                   nogdt.num_teams_act_week as num_teams_act_week_long,
                                   nogdt.num_captain_teams_act_week as num_cap_teams_act_week_long
                              from num_of_game_days_together nogdt,
                                   punter p,
                                   ca_time_day ctd
                             where nogdt.punter_punter_id=p.punter_id
                               and nogdt.ca_time_day_id=ctd.id
                               and ctd.year_week=l_r_long_first_year_week
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               ),
             week_act_join_short as (
                            select coalesce(aw.punter_key,wshort.punter_key) as punter_key,
                                   coalesce(aw.punter_id,wshort.punter_id) as punter_id,
                                   aw.nogd_member_act_week,
                                   aw.nogd_player_act_week,
                                   aw.nogd_captain_act_week,
                                   aw.nogd_team_agg_act_week,
                                   aw.num_teams_act_week,
                                   aw.num_captain_teams_act_week,
                                   wshort.nogd_member_act_week_short,
                                   wshort.nogd_player_act_week_short,
                                   wshort.nogd_captain_act_week_short,
                                   wshort.nogd_team_agg_act_week_short,
                                   wshort.num_teams_act_week_short,
                                   wshort.num_cap_teams_act_week_short
                              from act_week aw full outer join week_short wshort
                             on (aw.punter_key=wshort.punter_key)
                               ),
             week_act_join_short_long as (
                            select coalesce(wajs.punter_key,wlong.punter_key) as punter_key,
                                   coalesce(wajs.punter_id,wlong.punter_id) as punter_id,
                                   wajs.nogd_member_act_week,
                                   wajs.nogd_player_act_week,
                                   wajs.nogd_captain_act_week,
                                   wajs.nogd_team_agg_act_week,
                                   wajs.nogd_member_act_week_short,
                                   wajs.nogd_player_act_week_short,
                                   wajs.nogd_captain_act_week_short,
                                   wajs.nogd_team_agg_act_week_short,
                                   wajs.num_teams_act_week,
                                   wajs.num_captain_teams_act_week,
                                   wajs.num_teams_act_week_short,
                                   wajs.num_cap_teams_act_week_short,
                                   wlong.nogd_member_act_week_long,
                                   wlong.nogd_player_act_week_long,
                                   wlong.nogd_captain_act_week_long,
                                   wlong.nogd_team_agg_act_week_long,
                                   wlong.num_teams_act_week_long,
                                   wlong.num_cap_teams_act_week_long
                              from week_act_join_short wajs full outer join week_long wlong
                             on (wajs.punter_key=wlong.punter_key)
                               ),
                cust_agg_last_week as -- Föregående vecka
                            (select p.punter_key,
                                    p.punter_id,
                                   nogdt.nogd_member_act_week,
                                   nogdt.nogd_member_r13_week,
                                   nogdt.nogd_member_r52_week,
                                   nogdt.nogd_member_total,
                                   nogdt.nogd_player_act_week,
                                   nogdt.nogd_player_r13_week,
                                   nogdt.nogd_player_r52_week,
                                   nogdt.nogd_player_total,
                                   nogdt.nogd_captain_act_week,
                                   nogdt.nogd_captain_r13_week,
                                   nogdt.nogd_captain_r52_week,
                                   nogdt.nogd_captain_total,
                                   nogdt.nogd_team_agg_act_week,
                                   nogdt.nogd_team_agg_r13_week,
                                   nogdt.nogd_team_agg_r52_week,
                                   nogdt.nogd_team_agg_total,
                                   nogdt.NUM_TEAMS_ACT_WEEK,
                                   nogdt.NUM_TEAMS_R13_WEEK,
                                   nogdt.NUM_TEAMS_R52_WEEK,
                                   nogdt.NUM_TEAMS_TOTAL,
                                   nogdt.NUM_CAPTAIN_TEAMS_ACT_WEEK,
                                   nogdt.NUM_CAPTAIN_TEAMS_R13_WEEK,
                                   nogdt.NUM_CAPTAIN_TEAMS_R52_WEEK,
                                   nogdt.NUM_CAPTAIN_TEAMS_TOTAL
                              from num_of_game_days_together nogdt,
                                   punter p,
                                   ca_time_day ctd
                             where nogdt.punter_punter_id=p.punter_id
                               and nogdt.ca_time_day_id=ctd.id
                               and ctd.year_week=l_last_year_week
                               and p.kontotyp=TYPE_OF_ACCOUNT
                               ),
             week_act_short_long_lw as (
                            select coalesce(wajsl.punter_key,calw.punter_key) as punter_key,
                                   coalesce(wajsl.punter_id,calw.punter_id) as punter_punter_id,
                                   coalesce(wajsl.nogd_member_act_week,0) as nogd_member_act_week,
                                   coalesce(wajsl.nogd_player_act_week,0) as nogd_player_act_week,
                                   coalesce(wajsl.nogd_captain_act_week,0) as nogd_captain_act_week,
                                   coalesce(wajsl.nogd_team_agg_act_week,0) as nogd_team_agg_act_week,
                                   coalesce(wajsl.num_teams_act_week,0) as num_teams_act_week,
                                   coalesce(wajsl.num_captain_teams_act_week,0) as num_captain_teams_act_week,
                                   coalesce(calw.nogd_member_r13_week,0)+coalesce(wajsl.nogd_member_act_week,0)-coalesce(wajsl.nogd_member_act_week_short,0) as NOGD_MEMBER_R13_WEEK,
                                   coalesce(calw.nogd_player_r13_week,0)+coalesce(wajsl.nogd_player_act_week,0)-coalesce(wajsl.nogd_player_act_week_short,0) as NOGD_PLAYER_R13_WEEK,
                                   coalesce(calw.nogd_captain_r13_week,0)+coalesce(wajsl.nogd_captain_act_week,0)-coalesce(wajsl.nogd_captain_act_week_short,0) as NOGD_CAPTAIN_R13_WEEK,
                                   coalesce(calw.nogd_team_agg_r13_week,0)+coalesce(wajsl.nogd_team_agg_act_week,0)-coalesce(wajsl.nogd_team_agg_act_week_short,0)  as NOGD_TEAM_AGG_R13_WEEK,
                                   coalesce(calw.NUM_TEAMS_R13_WEEK,0)+coalesce(wajsl.num_teams_act_week,0)-coalesce(wajsl.num_teams_act_week_short,0) as NUM_TEAMS_R13_WEEK,
                                   coalesce(calw.NUM_CAPTAIN_TEAMS_R13_WEEK,0)+coalesce(wajsl.num_captain_teams_act_week,0)-coalesce(wajsl.num_cap_teams_act_week_short,0) as NUM_CAPTAIN_TEAMS_R13_WEEK,
                                   coalesce(calw.nogd_member_r52_week,0)+coalesce(wajsl.nogd_member_act_week,0)-coalesce(wajsl.nogd_member_act_week_long,0) as NOGD_MEMBER_R52_WEEK,
                                   coalesce(calw.nogd_player_r52_week,0)+coalesce(wajsl.nogd_player_act_week,0)-coalesce(wajsl.nogd_player_act_week_long,0) as NOGD_PLAYER_R52_WEEK,
                                   coalesce(calw.nogd_captain_r52_week,0)+coalesce(wajsl.nogd_captain_act_week,0)-coalesce(wajsl.nogd_captain_act_week_long,0) as NOGD_CAPTAIN_R52_WEEK,
                                   coalesce(calw.nogd_team_agg_r52_week,0)+coalesce(wajsl.nogd_team_agg_act_week,0)-coalesce(wajsl.nogd_team_agg_act_week_long,0) as NOGD_TEAM_AGG_R52_WEEK,
                                   coalesce(calw.NUM_TEAMS_R52_WEEK,0)+coalesce(wajsl.num_teams_act_week,0)-coalesce(wajsl.num_teams_act_week_long,0) as NUM_TEAMS_R52_WEEK,
                                   coalesce(calw.NUM_CAPTAIN_TEAMS_R52_WEEK,0)+coalesce(wajsl.num_captain_teams_act_week,0)-coalesce(wajsl.num_cap_teams_act_week_long,0) as NUM_CAPTAIN_TEAMS_R52_WEEK,
                                   coalesce(calw.nogd_member_total,0)+coalesce(wajsl.nogd_member_act_week,0) as NOGD_MEMBER_TOTAL,
                                   coalesce(calw.nogd_player_total,0)+coalesce(wajsl.nogd_player_act_week,0) as NOGD_PLAYER_TOTAL,
                                   coalesce(calw.nogd_captain_total,0)+coalesce(wajsl.nogd_captain_act_week,0) as NOGD_CAPTAIN_TOTAL,
                                   coalesce(calw.nogd_team_agg_total,0)+coalesce(wajsl.nogd_team_agg_act_week,0) as NOGD_TEAM_AGG_TOTAL,
                                   coalesce(calw.NUM_TEAMS_total,0)+coalesce(wajsl.num_teams_act_week,0) as NUM_TEAMS_total,
                                   coalesce(calw.NUM_CAPTAIN_TEAMS_TOTAL,0)+coalesce(wajsl.num_captain_teams_act_week,0) as NUM_CAPTAIN_TEAMS_TOTAL
                              from week_act_join_short_long wajsl full outer join cust_agg_last_week calw
                                on (wajsl.punter_key=calw.punter_key)
                             )
                             select l__act_week_id AS ca_time_day_id,
                                    punter_punter_id,
                                    nogd_member_act_week,
                                    nogd_member_r13_week,
                                    nogd_member_r52_week,
                                    nogd_member_total,
                                    nogd_player_act_week,
                                    nogd_player_r13_week,
                                    nogd_player_r52_week,
                                    nogd_player_total,
                                    nogd_captain_act_week,
                                    nogd_captain_r13_week,
                                    nogd_captain_r52_week,
                                    nogd_captain_total,
                                    nogd_team_agg_act_week,
                                    nogd_team_agg_r13_week,
                                    nogd_team_agg_r52_week,
                                    nogd_team_agg_total,
                                    num_teams_act_week,
                                    num_teams_r13_week,
                                    num_teams_r52_week,
                                    num_teams_total,
                                    num_captain_teams_act_week,
                                    num_captain_teams_r13_week,
                                    num_captain_teams_r52_week,
                                    num_captain_teams_total
                               from week_act_short_long_lw;
                               
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 2,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);                               

    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_nogd_together;

-------------------------------- load_cust_together_kpi ---------------------------------

procedure load_cust_together_kpi (p_date date) is

   l_is_last_day_of_week number;
   l__act_week_id number;
   l_day_exists number;
   l_act_year_week number;
   l_last_year_week number;
   l_r_short_first_year_week number;
   l_r_long_first_year_week number;
   l_insert boolean := true;
   ld_rolling_short_week date;

  MODULE_NAME            constant varchar2(30) := 'load_cust_together_kpi';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;


 begin

    dbms_output.put_line('början');

    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id, year_week
      into l_is_last_day_of_week, l__act_week_id, l_act_year_week
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from customer_together_kpi
        where ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    --First date of the last ROLLING_SHORT_WEEK
    if l_insert then
       ld_rolling_short_week := p_date-((ROLLING_SHORT_WEEK*7)+6); -- Sunday date 13 weeks ago.
    end if;


    if l_insert then
        ld_start_time:=sysdate;
        
      
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => null,
                                 pio_id => ln_batch_log_id,
                                 pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));    
    
        insert into customer_together_kpi(ca_time_day_id,
                                          punter_punter_id,
                                          revenue_shares,
                                          nogd_player_shares,
                                          num_of_teams,
                                          num_of_active_teams,
                                          num_of_captain_teams,
                                          num_of_active_captain_teams)
         with revenue_shares_tab_1 as
                    (select acc_index,
                            case when rev_together_r13!=0 then
                                    rev_together_r13/revenue_tot_r13
                                 else
                                    0 -- Allt spel på andra kanaler än tillsammans
                                 end revenue_shares
                        from (
                      select p.acc_index,
                             sum(case when tech_channel_id in (14,16,96) then revenue_r13_week else 0 end) as rev_together_r13, --1, 3 -- Ej ombudslag (70)
                             sum(revenue_r13_week) as revenue_tot_r13
                        from customer_revenue cr inner join punter p
                          on p.punter_id=cr.punter_punter_id
                         and cr.ca_time_day_id=l__act_week_id
                       group by p.acc_index)),
              nogd_player_shares_tab_2 as
                      (select acc_index,
                              case when nogd_player_r13_week!=0 then --
                                   nogd_player_r13_week/nogd_team_agg_r13_week
                              else
                                   0
                              end as nogd_player_shares
                         from num_of_game_days_together a inner join punter p
                           on p.punter_id=a.punter_punter_id
                          and a.ca_time_day_id=l__act_week_id),
              join_1_2 as
                     (select coalesce(tab_1.acc_index,tab_2.acc_index) acc_index,
                             coalesce(tab_1.revenue_shares,0) as revenue_shares,
                             coalesce(tab_2.nogd_player_shares,0) as nogd_player_shares
                        from revenue_shares_tab_1 tab_1 full outer join nogd_player_shares_tab_2 tab_2
                          on tab_1.acc_index=tab_2.acc_index),
              num_of_teams_tab_3 as
                    (select lm.konto_kontoindex,
                            count(distinct tl.punter_key) num_of_teams
                       from mdb.lagmedlemmar lm inner join mdb.tillsammans_lag tl
                         on tl.kontoindex=lm.tillsammans_lag_kontoindex
                        and (lm.slutdatum is null or lm.slutdatum > p_date)
                      group by lm.konto_kontoindex),
              join_1_2_3 as
                    (select coalesce(join_1_2.acc_index,tab_3.konto_kontoindex) as acc_index,
                            coalesce(join_1_2.revenue_shares,0) as revenue_shares,
                            coalesce(join_1_2.nogd_player_shares,0) as nogd_player_shares,
                            coalesce(tab_3.num_of_teams,0) as num_of_teams
                      from join_1_2 full outer join num_of_teams_tab_3 tab_3
                         on join_1_2.acc_index=tab_3.konto_kontoindex),
              num_of_active_teams_tab_4 as
                    (select lm.konto_kontoindex,
                            count(distinct tl.punter_key) num_of_active_teams
                       from mdb.lagmedlemmar lm inner join latest_team_game_date_together ltgdt
                         on lm.tillsammans_lag_kontoindex=ltgdt.team_account_index
                            inner join mdb.tillsammans_lag tl
                         on tl.kontoindex=lm.tillsammans_lag_kontoindex
                        and ltgdt.ca_time_day_id=l__act_week_id
                        and ltgdt.latest_game_date >= ld_rolling_short_week
                        and (lm.slutdatum is null or lm.slutdatum > p_date)
                      group by lm.konto_kontoindex),
              join_1_2_3_4 as
                    (select coalesce(join_1_2_3.acc_index,tab_4.konto_kontoindex) as acc_index,
                            coalesce(join_1_2_3.revenue_shares,0) as revenue_shares,
                            coalesce(join_1_2_3.nogd_player_shares,0) as nogd_player_shares,
                            coalesce(join_1_2_3.num_of_teams,0) as num_of_teams,
                            coalesce(tab_4.num_of_active_teams,0) as num_of_active_teams
                      from join_1_2_3 full outer join num_of_active_teams_tab_4 tab_4
                        on join_1_2_3.acc_index=tab_4.konto_kontoindex),
              num_of_captain_teams_tab_5 as
                    (select tl.lagledare_id,
                            count(distinct tl.punter_key) num_of_captain_teams
                       from mdb.tillsammans_lag tl
                      where tl.status='Aktivt'
                      group by tl.lagledare_id),
              join_1_2_3_4_5 as
                    (select coalesce(join_1_2_3_4.acc_index,tab_5.lagledare_id) as acc_index,
                            coalesce(join_1_2_3_4.revenue_shares,0) as revenue_shares,
                            coalesce(join_1_2_3_4.nogd_player_shares,0) as nogd_player_shares,
                            coalesce(join_1_2_3_4.num_of_teams,0) as num_of_teams,
                            coalesce(join_1_2_3_4.num_of_active_teams,0) as num_of_active_teams,
                            coalesce(tab_5.num_of_captain_teams,0) as num_of_captain_teams
                      from join_1_2_3_4 full outer join num_of_captain_teams_tab_5 tab_5
                        on join_1_2_3_4.acc_index=tab_5.lagledare_id),
              num_of_active_cap_teams_tab_6 as
                    (select tl.lagledare_id,
                            count(distinct tl.punter_key) num_of_active_captain_teams
                       from latest_team_game_date_together ltgdt inner join mdb.tillsammans_lag tl
                         on ltgdt.team_account_index=tl.kontoindex
                        and tl.status='Aktivt'
                        and ltgdt.ca_time_day_id=l__act_week_id
                        and ltgdt.latest_game_date >= ld_rolling_short_week
                      group by tl.lagledare_id),
              join_1_2_3_4_5_6 as
                    (select coalesce(join_1_2_3_4_5.acc_index,tab_6.lagledare_id) as acc_index,
                            coalesce(join_1_2_3_4_5.revenue_shares,0) as revenue_shares,
                            coalesce(join_1_2_3_4_5.nogd_player_shares,0) as nogd_player_shares,
                            coalesce(join_1_2_3_4_5.num_of_teams,0) as num_of_teams,
                            coalesce(join_1_2_3_4_5.num_of_active_teams,0) as num_of_active_teams,
                            coalesce(join_1_2_3_4_5.num_of_captain_teams,0) as num_of_captain_teams,
                            coalesce(tab_6.num_of_active_captain_teams,0) as num_of_active_captain_teams
                       from join_1_2_3_4_5 full outer join num_of_active_cap_teams_tab_6 tab_6
                         on join_1_2_3_4_5.acc_index=tab_6.lagledare_id),
              punter_current_tab as
                    (select pc.acc_index as acc_index,
                            pc.punter_id as PUNTER_PUNTER_ID,
                            join_1_2_3_4_5_6.revenue_shares as revenue_shares,
                            join_1_2_3_4_5_6.nogd_player_shares as nogd_player_shares,
                            join_1_2_3_4_5_6.num_of_teams as num_of_teams,
                            join_1_2_3_4_5_6.num_of_active_teams as num_of_active_teams,
                            join_1_2_3_4_5_6.num_of_captain_teams as num_of_captain_teams,
                            join_1_2_3_4_5_6.num_of_active_captain_teams as num_of_active_captain_teams
                       from join_1_2_3_4_5_6 inner join punter_current pc
                         on join_1_2_3_4_5_6.acc_index=pc.acc_index)
            select l__act_week_id as ca_time_day_id,
                    punter_punter_id,
                    revenue_shares,
                    nogd_player_shares,
                    num_of_teams,
                    num_of_active_teams,
                    num_of_captain_teams,
                    num_of_active_captain_teams
              from punter_current_tab
             where num_of_teams>0;
             
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);             

    end if;

 exception
    when others then
         dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
         raise;
 end load_cust_together_kpi;


-------------------------------------------------------------------------------
procedure load_nogd_channel_rev(p_date date) is

   last_day_of_week_indicator pls_integer;
   this_week_last_day_id      pls_integer;
   row_count                  pls_integer;

   this_week                  pls_integer;
   last_week                  pls_integer;
   r13_week                   pls_integer;
   r52_week                   pls_integer;


  MODULE_NAME            constant varchar2(30) := 'load_nogd_channel_rev';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from num_of_game_days_channel_rev
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

  select year_week into last_week from ca_time_day
  where full_date = p_date - SEVEN;

  select year_week into r13_week from ca_time_day
  where full_date = p_date - (SEVEN * ROLLING_SHORT_WEEK);

  select year_week into r52_week  from ca_time_day
  where full_date = p_date - (SEVEN * ROLLING_LONG_WEEK);

  insert into num_of_game_days_channel_rev(
      ca_time_day_id,
      punter_punter_id,
      punter_key,
      channel_channel_id,
      partner_channel_id,
      num_of_game_days_wtd,
      num_of_game_days_r13_week,
      num_of_game_days_r52_week,
      num_of_game_days_total,
      revenue_wtd,
      revenue_r13_week,
      revenue_r52_week,
      revenue_total)
  with

  sub1 as(
    select
      punter_key,
      max(max(punter_id)) over(partition by punter_key)    as punter_punter_id,
      channel_channel_id,
      partner_channel_id,
      count(distinct full_date)      as num_of_game_days,
      sum(revenue)                    as revenue
    from customer_activity2,
          ca_time_day,
          punter
    where customer_activity2.ca_time_day_id = ca_time_day.id
      and customer_activity2.punter_punter_id = punter.punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_time_day.year_week = this_week
    group by
      punter_key,
      channel_channel_id,
      partner_channel_id),

  sub2 as(
    select
      punter_key,
      max(max(punter_id)) over(partition by punter_key)    as punter_punter_id,
      channel_channel_id,
      partner_channel_id,
      count(distinct full_date)      as num_of_game_days,
      sum(revenue)                    as revenue
    from customer_activity2,
          ca_time_day,
          punter
    where customer_activity2.ca_time_day_id = ca_time_day.id
      and customer_activity2.punter_punter_id = punter.punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_time_day.year_week = r52_week
    group by
      punter_key,
      channel_channel_id,
      partner_channel_id),

  sub3 as(
    select
      punter_key,
      max(max(punter_id)) over(partition by punter_key)    as punter_punter_id,
      channel_channel_id,
      partner_channel_id,
      count(distinct full_date)      as num_of_game_days,
      sum(revenue)                    as revenue
    from customer_activity2,
          ca_time_day,
          punter
    where customer_activity2.ca_time_day_id = ca_time_day.id
      and customer_activity2.punter_punter_id = punter.punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_time_day.year_week = r13_week
    group by
      punter_key,
      channel_channel_id,
      partner_channel_id),

  sub4 as(
    select
      coalesce(sub1.punter_key, sub2.punter_key)                       as punter_key,
      coalesce(sub1.punter_punter_id, sub2.punter_punter_id)           as punter_punter_id,
      coalesce(sub1.channel_channel_id, sub2.channel_channel_id)       as channel_channel_id,
      coalesce(sub1.partner_channel_id, sub2.partner_channel_id)       as partner_channel_id,
      coalesce(sub1.num_of_game_days, ZERO)                            as num_of_game_days,
      coalesce(sub2.num_of_game_days, ZERO)                            as num_of_game_days_r52_week,
      coalesce(sub1.revenue,0)                                         as revenue,
      coalesce(sub2.revenue,0)                                         as revenue_r52_week
    from sub1 full outer join sub2
      on sub1.punter_key = sub2.punter_key and
         sub1.channel_channel_id = sub2.channel_channel_id and
        sub1.partner_channel_id = sub2.partner_channel_id),

  sub5 as(
    select coalesce(sub4.punter_key, sub3.punter_key)                 as punter_key,
      coalesce(sub4.punter_punter_id, sub3.punter_punter_id)           as punter_punter_id,
      coalesce(sub4.channel_channel_id, sub3.channel_channel_id)       as channel_channel_id,
      coalesce(sub4.partner_channel_id, sub3.partner_channel_id)       as partner_channel_id,
      coalesce(sub4.num_of_game_days, ZERO)                            as num_of_game_days,
      coalesce(sub4.num_of_game_days_r52_week, ZERO)                   as num_of_game_days_r52_week,
      coalesce(sub3.num_of_game_days, ZERO)                            as num_of_game_days_r13_week,
      coalesce(sub4.revenue,0)                                         as revenue,
      coalesce(sub4.revenue_r52_week,0)                                as revenue_r52_week,
      coalesce(sub3.revenue,0)                                         as revenue_r13_week
    from sub3 full outer join sub4
      on sub3.punter_key = sub4.punter_key and
         sub3.channel_channel_id = sub4.channel_channel_id and
        sub3.partner_channel_id = sub4.partner_channel_id),

  sub6 as(
    select
      punter.punter_key                          as punter_key,
      punter_punter_id                           as punter_punter_id,
      channel_channel_id                         as channel_channel_id,
      partner_channel_id                         as partner_channel_id,
      num_of_game_days_r13_week                  as num_of_game_days_r13_week,
      num_of_game_days_r52_week                  as num_of_game_days_r52_week,
      num_of_game_days_total                     as num_of_game_days_total,
      revenue_r13_week                           as revenue_r13_week,
      revenue_r52_week                           as revenue_r52_week,
      revenue_total                              as revenue_total
    from num_of_game_days_channel_rev,
          ca_time_day,
          punter
    where ca_time_day_id = ca_time_day.id
      and punter_punter_id = punter_id
      and kontotyp = TYPE_OF_ACCOUNT
      and ca_time_day.year_week = last_week),

  sub7 as(
    select
      coalesce(sub6.punter_punter_id, sub5.punter_punter_id)  as punter_punter_id,
      coalesce(sub6.punter_key, sub5.punter_key)  as punter_key,
      coalesce(sub6.channel_channel_id, sub5.channel_channel_id)
                                                               as channel_channel_id,
      coalesce(sub6.partner_channel_id, sub5.partner_channel_id)
                                                               as partner_channel_id,
      coalesce(sub5.num_of_game_days, ZERO)                   as num_of_game_days_wtd,

      coalesce(sub6.num_of_game_days_r13_week, ZERO) + coalesce(sub5.num_of_game_days, ZERO) -
        coalesce(sub5.num_of_game_days_r13_week, ZERO)        as num_of_game_days_r13,

      coalesce(sub6.num_of_game_days_r52_week, ZERO) + coalesce(sub5.num_of_game_days, ZERO) -
        coalesce(sub5.num_of_game_days_r52_week, ZERO)        as num_of_game_days_r52,

      coalesce(sub6.num_of_game_days_total, ZERO) + coalesce(sub5.num_of_game_days, ZERO)
                                                               as num_of_game_days_total,
     coalesce(sub5.revenue,0) as revenue_wtd,
     coalesce(sub6.revenue_r13_week,0)+coalesce(sub5.revenue,0)-coalesce(sub5.revenue_r13_week,0) as revenue_r13_week,
     coalesce(sub6.revenue_r52_week,0)+coalesce(sub5.revenue,0)-coalesce(sub5.revenue_r52_week,0) as revenue_r52_week,
     coalesce(sub6.revenue_total,0)+coalesce(sub5.revenue,0) as revenue_total
    from sub5 full outer join sub6
    on  sub5.punter_key = sub6.punter_key and
        sub5.channel_channel_id = sub6.channel_channel_id and
        sub5.partner_channel_id = sub6.partner_channel_id)

    select
      this_week_last_day_id,
      punter_punter_id,
      punter_key,
      channel_channel_id,
      partner_channel_id,
      sum(num_of_game_days_wtd) as num_of_game_days_wtd,
      sum(num_of_game_days_r13) as num_of_game_days_r13,
      sum(num_of_game_days_r52) as num_of_game_days_r52,
      sum(num_of_game_days_total) as num_of_game_days_total,
      sum(revenue_wtd) as revenue_wtd,
      sum(revenue_r13_week) as revenue_r13_week,
      sum(revenue_r52_week) as revenue_r52_week,
      sum(revenue_total) as revenue_total
    from sub7
    group by this_week_last_day_id,
             punter_punter_id,
             punter_key,
             channel_channel_id,
             partner_channel_id;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_nogd_channel_rev;

------------------------------------------------------------------------------

procedure load_customer_cluster_1(p_date date) is

  l_is_last_day_of_week number;
  l__act_week_id number;
  l_day_exists number;
  l_insert boolean := true;

  VR constant pls_integer := 1012; --Cluster VR

  MODULE_NAME            constant varchar2(30) := 'load_customer_cluster_1';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

begin

    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id
      into l_is_last_day_of_week, l__act_week_id
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
    if l_insert then
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from customer_cluster
        where ca_cluster_id not in (select distinct ca_cluster_id
                                      from ca_cluster_concept ccc
                                           inner join ca_priority cp
                                        on (cp.id=ccc.ca_priority_id)
                                     where priority in (1,2,3))
          and ca_time_day_id=l__act_week_id
          and rownum<2;
        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;
    end if;

    if l_insert then
    
        ld_start_time:=sysdate;
        
      
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => null,
                                 pio_id => ln_batch_log_id,
                                 pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));
                                 

        INSERT INTO gtt_num_of_game_days 
                     select  a.ca_time_day_id,
                             a.punter_punter_id,
                             b.punter_key,
                             a.ca_game_type_id,
                             a.ca_race_type_id,
                             a.ca_program_concept_id,
                             a.ca_channel_type_id,
                             a.ca_bet_method_group_id,
                             a.tech_channel_id,
                             a.num_of_game_days_wtd,
                             a.num_of_game_days_r13_week,
                             a.num_of_game_days_r52_week,
                             a.num_of_game_days_total
                        from num_of_game_days a inner join punter b
                          on (a.punter_punter_id=b.punter_id
                         and a.ca_time_day_id=l__act_week_id);
                         

               INSERT INTO gtt_num_of_game_days_total
                      select c.ca_time_day_id,
                             c.punter_punter_id,
                             d.punter_key,
                             c.num_of_game_days_wtd,
                             c.num_of_game_days_r13_week,
                             c.num_of_game_days_r52_week,
                             c.num_of_game_days_total
                        from num_of_game_days_total c inner join punter d
                          on (c.punter_punter_id = d.punter_id
                         and c.ca_time_day_id=l__act_week_id);
                                 
    
        insert into customer_cluster(punter_punter_id,
                                     ca_time_day_id,
                                     ca_cluster_id,
                                     punter_key)
     select punter_punter_id, ca_time_day_id, ca_cluster_id, punter_key from (
         select punter_punter_id,
                ca_time_day_id,
                case when num_of_game_days_total < 5 then
                          1008  -- 1008 Inget kluster
                     when tech_channel_desc=1 and game_type_desc=1002 and gallop_horse_racing=1  then
                          1012	-- Endast Tillsammans-, Galopp- och VR-spelare
                     when tech_channel_desc=1 and game_type_desc!=1002 and gallop_horse_racing=1 then
                          1009	-- Endast Tillsammans- och Galopp-spelare
                     when tech_channel_desc=1 and game_type_desc=1002 and gallop_horse_racing!=1 then
                          1010	-- Endast Tillsammans- och VR-spelare
                     when tech_channel_desc!=1 and game_type_desc=1002 and gallop_horse_racing=1 then
                          1011	-- Endast Galopp- och VR-spelare
                     when tech_channel_desc=1 and game_type_desc!=1002 and gallop_horse_racing!=1  then
                          1005  -- 1005 Endast Tillsammans-spelare
                     when tech_channel_desc!=1 and game_type_desc=1002 and gallop_horse_racing!=1 then
                          1007  -- 1007 Endast VR-spelare
                     when tech_channel_desc!=1 and game_type_desc!=1002 and gallop_horse_racing=1 then
                          1006  -- 1006 Endast Galopp-spelare
                else null
                end ca_cluster_id,
                punter_key
         from (
              select ca_time_day_id,
                     punter_punter_id,
                     punter_key,
                     ca_game_type_id,
                     FIRST_VALUE(ca_game_type_id) OVER (PARTITION BY punter_punter_id ORDER BY ca_game_type_id desc) game_type_desc,
                     ca_race_type_id,
                     FIRST_VALUE(gallop_and_horse_racing) OVER (PARTITION BY punter_punter_id ORDER BY gallop_and_horse_racing desc) gallop_horse_racing,
                     tech_channel_id,
                     FIRST_VALUE(tech_channel_id) OVER (PARTITION BY punter_punter_id ORDER BY tech_channel_id desc) tech_channel_desc,
                     trot_and_horse_racing,
                     max(num_of_game_days_total) over(partition by punter_punter_id) as num_of_game_days_total,
                     FIRST_VALUE(trot_and_horse_racing) OVER (PARTITION BY punter_punter_id ORDER BY trot_and_horse_racing asc) AS trot_and_horse_racing_AF
               from (
                 select
                    nogd.ca_time_day_id,
                    nogd_tot.punter_punter_id,
                    nogd.punter_key,
                    nogd.ca_game_type_id,
                    nogd.ca_race_type_id,
                    nogd_tot.num_of_game_days_total,
                    case when tech_channel_id in (14,16,96,70) then -- Tillsammans
                       1 -- Kunden har spelat tillsammans
                    else
                       0 -- Kunden har spelat annat än via tillsammans
                    end tech_channel_id,
                    case when (ca_game_type_id=1001) and (ca_race_type_id=1001) and tech_channel_id not in (14,16,96,70) then --
                       0 -- Kunden har spelat vanligt på trav och ej tillsammans
                    else
                       1 -- Kunden kan ha spelat VR trav, galopp eller galopp vanligt, tillsammans (trav)
                    end trot_and_horse_racing,
                    case when (ca_game_type_id=1001) and (ca_race_type_id=1002) then --
                       1 -- Kunden har spelat vanligt på galopp
                    else
                       0
                    end gallop_and_horse_racing
                FROM
                    (select  ca_time_day_id,
                             punter_punter_id,
                             punter_key,
                             ca_game_type_id,
                             ca_race_type_id,
                             ca_program_concept_id,
                             ca_channel_type_id,
                             ca_bet_method_group_id,
                             tech_channel_id,
                             num_of_game_days_wtd,
                             num_of_game_days_r13_week,
                             num_of_game_days_r52_week,
                             num_of_game_days_total
                        from gtt_num_of_game_days) nogd,
                     (select ca_time_day_id,
                             punter_punter_id,
                             punter_key,
                             num_of_game_days_wtd,
                             num_of_game_days_r13_week,
                             num_of_game_days_r52_week,
                             num_of_game_days_total
                        from gtt_num_of_game_days_total) nogd_tot
                  where nogd_tot.punter_key=nogd.punter_key
                    and nogd_tot.ca_time_day_id=nogd.ca_time_day_id
                )) where trot_and_horse_racing_AF!=0)
                where ca_cluster_id is not null
                group by punter_punter_id, ca_time_day_id, ca_cluster_id, punter_key;
                

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

                
        end if;

exception
   when others then
        dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
        raise;
end load_customer_cluster_1;
------------------------------------------------------------------------------
procedure load_customer_cluster_2(p_date date) is

  l_is_last_day_of_week number;
  l__act_week_id number;
  l_day_exists number;
  l_insert boolean := true;

  VR constant pls_integer := 1012; --Cluster VR

  MODULE_NAME            constant varchar2(30) := 'load_customer_cluster_2';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;

begin
    -- l_is_last_day_of_week: is the date the last day of the week
    -- l_id: id for last day of week
    -- l_act_year_week: year_week for the actual week
    select is_last_day_of_week, id
      into l_is_last_day_of_week, l__act_week_id
      from ca_time_day
     where full_date=p_date;

    -- Check last day of week
    if l_is_last_day_of_week=0 then
       l_insert:=false; --0 is not the last day of week and the process will not insert any rows
       dbms_output.put_line('Ej söndag');
    end if;

    -- Check if week already exists
--    DENNA PROCEDUR LIGGER SOM NUMMER 2 VILKET GÖR ATT TABELLEN REDAN INNEHÅLLER DATA FÖR VECKAN
-- ALTERNATIVET ÄR ATT KONTROLLERA ALLA KLUSTER SOM DENNA PROCUDUR LÄGGER IN.
    if l_insert then
    
        ld_start_time:=sysdate;
        
      
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => null,
                                 pio_id => ln_batch_log_id,
                                 pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));
    
       select /*+ first_rows(1) */ count(distinct ca_time_day_id)
         into l_day_exists
         from customer_cluster
        where ca_time_day_id=l__act_week_id
          and ca_cluster_id in (select distinct ca_cluster_id
                                  from ca_cluster_concept ccc
                                       inner join ca_priority cp
                                    on (cp.id=ccc.ca_priority_id)
                                 where priority in (1,2,3))
          and rownum<2;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

           ln_batch_log_id:=null;

        -- 1 means the table already have data and the process will not insert any rows
        if l_day_exists=1 then
           l_insert := false;
           dbms_output.put_line('Rad finns redan');
        end if;

    end if;


    if l_insert then
    
        ld_start_time:=sysdate;
        
      
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 2,
                                 pin_no_rows => null,
                                 pio_id => ln_batch_log_id);    
    
        insert into customer_cluster(punter_punter_id,
                                     ca_time_day_id,
                                     ca_cluster_id,
                                     punter_key)
              with
                  tot as (
                         select b.punter_key,
                                punter_punter_id AS punter_id,
                                num_of_game_days_r52_week,    -- Kunden har spelat 10 dagar eller fler på trav senaste 52 veckorna
                                num_of_game_days_total       -- Kunden har spelat färre än 52 dagar senaste 52 veckorna
                           from num_of_game_days_race_type a, punter b
                          where ca_time_day_id=l__act_week_id
                            and b.kontotyp=TYPE_OF_ACCOUNT
                            and a.ca_race_type_id=TROT_ID
                            and a.punter_punter_id=b.punter_id
                            and b.punter_key not in (select cc.punter_key from customer_cluster cc where cc.ca_time_day_id=l__act_week_id)),
            trot_concept as (
                         select b.punter_key,
                                c.id,
                                c.concept,
                                num_of_game_days_r52_week,    -- Kunden har spelat 10 dagar eller fler på trav senaste 52 veckorna
                                num_of_game_days_total,       -- Kunden har spelat färre än 5 dagar senaste 52 veckorna
                               -- case when c.id in (1006) then -- V75-dagar PRIO1
                                case when is_program_concept_priority(c.id, 1) = 1 then
                                        1
                                     when c.id = VR then
                                        -1 -- VR ska ej räknas med
                                     else
                                        0
                                end id_prio1,
                              --  case when c.id in (1006,1008) then -- V75-dagar och Extra V75 PRIO2
                               case when is_program_concept_priority(c.id, 2) = 1 then
                                        2
                                     when c.id = VR then
                                        -1 -- VR ska ej räknas med
                                     else
                                        0
                                end id_prio2,
                              --  case when c.id in (1006,1008,1010) then -- V75-dagar, Extra V75 och V86-dagar PRIO3
                              case when is_program_concept_priority(c.id, 3) = 1 then
                                        3
                                     when c.id = VR then
                                        -1 -- VR ska ej räknas med
                                     else
                                        0
                                end id_prio3
                           from num_of_game_days_trot_concept a, punter b, ca_program_concept c
                          where ca_time_day_id=l__act_week_id
                            and b.kontotyp=TYPE_OF_ACCOUNT
                            and a.punter_punter_id=b.punter_id
                            and a.ca_program_concept_id=c.id),
            Prio_cluster as (
                         select 
                           trot_concept.punter_key                     as punter_key,
                           tot.punter_key                              as punter_key_tot,
                           tot.punter_id                               as punter_id,
                           trot_concept.id                             as ca_program_concept_id,
                           trot_concept.id_prio1                       as ca_program_concept_id_prio1,
                           trot_concept.id_prio2                       as ca_program_concept_id_prio2,
                           trot_concept.id_prio3                       as ca_program_concept_id_prio3,
                           trot_concept.concept                        as program_concept,
                           trot_concept.num_of_game_days_r52_week      as concept_52,
                           trot_concept.num_of_game_days_total         as concept_tot,
                           tot.num_of_game_days_r52_week               as tot_52,
                           tot.num_of_game_days_total                  as tot_tot
                         from trot_concept inner join tot
                           on trot_concept.punter_key = tot.punter_key),
            cluster_result as (
                    select  punter_key,
                           punter_key_tot,
                           punter_id,
                           ca_program_concept_id,
                           ca_program_concept_id_prio1,
                           ca_program_concept_id_prio2,
                           ca_program_concept_id_prio3,
                           program_concept,
                           concept_52,
                           concept_tot,
                           tot_52,
                           tot_tot,
                           case when tot_52 >= get_num_of_game_days(ONE) then
                              sum(concept_52) over(partition by punter_key, ca_program_concept_id_prio1)/tot_52
                           else
                              sum(concept_tot) over(partition by punter_key, ca_program_concept_id_prio1)/tot_tot
                           end concept_quota_prio1, -- Endast relevant när det är V75-dagar
                           case when tot_52 >= get_num_of_game_days(TWO) then
                              sum(concept_52) over(partition by punter_key, ca_program_concept_id_prio2)/tot_52
                           else
                              sum(concept_tot) over(partition by punter_key, ca_program_concept_id_prio2)/tot_tot
                           end concept_quota_prio2,
                           case when tot_52 >= get_num_of_game_days(THREE) then
                              sum(concept_52) over(partition by punter_key, ca_program_concept_id_prio3)/tot_52
                           else
                              sum(concept_tot) over(partition by punter_key, ca_program_concept_id_prio3)/tot_tot
                           end concept_quota_prio3
                     from Prio_cluster
                    order by punter_key),
    cluster_filter as (
            select punter_key,
                           punter_key_tot,
                           punter_id,
                           ca_program_concept_id,
                           ca_program_concept_id_prio1,
                           ca_program_concept_id_prio2,
                           ca_program_concept_id_prio3,
                           program_concept,
                           concept_52,
                           concept_tot,
                           tot_52,
                           tot_tot,
                           FIRST_VALUE(concept_quota_prio1) OVER (PARTITION BY punter_key ORDER BY ca_program_concept_id_prio1 desc) AS c1_quota,
                           FIRST_VALUE(concept_quota_prio2) OVER (PARTITION BY punter_key ORDER BY ca_program_concept_id_prio2 desc) AS c2_quota,
                           FIRST_VALUE(concept_quota_prio3) OVER (PARTITION BY punter_key ORDER BY ca_program_concept_id_prio3 desc) AS c3_quota,
                           FIRST_VALUE(ca_program_concept_id_prio1) OVER (PARTITION BY punter_key ORDER BY ca_program_concept_id_prio1 desc) AS c1_grp,
                           FIRST_VALUE(ca_program_concept_id_prio2) OVER (PARTITION BY punter_key ORDER BY ca_program_concept_id_prio2 desc) AS c2_grp,
                           FIRST_VALUE(ca_program_concept_id_prio3) OVER (PARTITION BY punter_key ORDER BY ca_program_concept_id_prio3 desc) AS c3_grp
                from cluster_result)
            select distinct punter_id,
                   l__act_week_id,
                   case when tot_tot < 5 then                  1008 -- 'Inget kluster'
                        when c1_grp>0 and c1_quota >= get_percentage(ONE) then 1001 -- 'V75-Lördags-spelaren'
                        when c2_grp>0 and c2_quota >= get_percentage(TWO) then 1002 -- 'V75-spelaren'
                        when c3_grp>0 and c3_quota >= get_percentage(THREE) then 1003 -- 'V75-V86-spelaren'
                   else 1004 --'Specialisten'
                   end ca_cluster_id,
                   punter_key
              from cluster_filter;
              
 
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 2,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);              

    end if;

exception
   when others then
        dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
        raise;
end load_customer_cluster_2;

------------------------------------------------------------------------------

procedure load_customer_frequency(p_date date) is

   last_day_of_week_indicator pls_integer;
   this_week_last_day_id      pls_integer;
   row_count                  pls_integer;

   this_week                  pls_integer;

  MODULE_NAME            constant varchar2(30) := 'load_customer_frequency';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from customer_frequency
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;


    INSERT INTO gtt_num_of_game_days_total
          select c.ca_time_day_id,
                 c.punter_punter_id,
                 d.punter_key,
                 c.num_of_game_days_wtd,
                 c.num_of_game_days_r13_week,
                 c.num_of_game_days_r52_week,
                 c.num_of_game_days_total
            from num_of_game_days_total c inner join punter d
              on (c.punter_punter_id = d.punter_id
             and c.ca_time_day_id=this_week_last_day_id);


  insert into customer_frequency(
      ca_time_day_id,
      punter_punter_id,
      ca_frequency_id)
      with cust_clust as (
              select
                ca_time_day_id,
                punter_punter_id,
                ca_cluster_id,
                punter.punter_key
          from  customer_cluster,
                punter
          where
              customer_cluster.punter_punter_id = punter.punter_id and
              customer_cluster.ca_time_day_id = this_week_last_day_id
            ),
            cust_nogd as (
              select
                    ca_time_day_id,
                    punter_punter_id,
                    num_of_game_days_wtd,
                    num_of_game_days_r13_week,
                    num_of_game_days_r52_week,
                    num_of_game_days_total,
                    punter_key
              from  gtt_num_of_game_days_total)
           select cust_nogd.ca_time_day_id         as ca_time_day_id,
                  cust_nogd.punter_punter_id       as punter_punter_id,
                  ca_frequency.id                   as ca_frequency_id
             from cust_nogd, cust_clust, ca_cluster_interval, ca_frequency
            where cust_clust.punter_key = cust_nogd.punter_key and
                  cust_clust.ca_time_day_id = cust_nogd.ca_time_day_id and
                  cust_clust.ca_cluster_id = ca_cluster_interval.ca_cluster_id and
                  num_of_game_days_r13_week >= min_val and
                  num_of_game_days_r13_week <= max_val and
                  ca_frequency_id = ca_frequency.id and
                  cust_clust.ca_time_day_id = this_week_last_day_id;
                  
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);                  

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_frequency;


------------------------------------------------------------------------------

procedure load_customer_rev_interval(p_date date) is

   last_day_of_week_indicator pls_integer;
   this_week_last_day_id      pls_integer;
   row_count                  pls_integer;

   this_week                  pls_integer;

  MODULE_NAME            constant varchar2(30) := 'load_customer_rev_interval';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from customer_revenue_interval
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));


          insert into customer_revenue_interval (ca_time_day_id,
                                                 punter_punter_id,
                                                 ca_revenue_interval_id)
          with rev_52 as
             (select cr.ca_time_day_id,
                     p.punter_key,
                     max(cr.punter_punter_id) punter_punter_id,
                     ceil(sum(cr.revenue_r52_week)) as rev_52_week
                from customer_revenue cr inner join punter p
                     on p.punter_id=cr.punter_punter_id
               where ca_time_day_id=this_week_last_day_id
               group by cr.ca_time_day_id,
                        p.punter_key)
            select r52.ca_time_day_id,
                   r52.punter_punter_id,
                   ca.id as ca_revenue_interval_id
              from rev_52 r52 inner join ca_revenue_interval ca
               on (r52.rev_52_week<=ca.revenue_to and r52.rev_52_week>=revenue_from);

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_rev_interval;

------------------------------------------------------------------------------

procedure load_customer_cycle(p_date date) is

  last_day_of_week_indicator pls_integer;
  this_week_last_day_id      pls_integer;
  row_count                  pls_integer;

  short_week                 date;

  this_week                  pls_integer;

  CY_ID_1001     constant  pls_integer := 1001;
  CY_ID_1002     constant  pls_integer := 1002;
  CY_ID_1003     constant  pls_integer := 1003;
  CY_ID_1004     constant  pls_integer := 1004;
  CY_ID_1005     constant  pls_integer := 1005;
  CY_ID_1006     constant  pls_integer := 1006;
  CY_ID_1007     constant  pls_integer := 1007;

  MODULE_NAME            constant varchar2(30) := 'load_customer_cycle';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from customer_cycle
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

   short_week := p_date - (SEVEN * ROLLING_SHORT_WEEK);

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 10011,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));


-- 1001
  insert into customer_cycle(
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    ca_time_day_id, max(punter_punter_id), CY_ID_1001 as ca_cycle_id
  from num_of_game_days_total, punter
  where
    punter_punter_id = punter_id and
    ca_time_day_id = this_week_last_day_id and
    NUM_OF_GAME_DAYS_R13_WEEK <= 4 and num_of_game_days_total <= 4 and
    startdate > to_char(short_week,'yyyymmdd') and
    kontotyp = 'PERSON'
  group by ca_time_day_id, punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 10011,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 10012,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);
                             
-- 1001 All members with no games played
  insert into customer_cycle (
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    this_week_last_day_id, max(punter_id), CY_ID_1001 as ca_cycle_id
  from punter
  where
    startdate > to_char(short_week,'yyyymmdd') and
    kontotyp = 'PERSON' and
    punter_key not in
      (select punter_key from num_of_game_days_total, punter p2
        where  punter_punter_id = p2.punter_id and
                ca_time_day_id = this_week_last_day_id)
  group by punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 10012,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1002,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);
                             
-- 1002
  insert into customer_cycle(
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    this_week_last_day_id, max(punter_id), CY_ID_1002 as ca_cycle_id
  from punter
  where
    punter_key not in
      (select punter_key from num_of_game_days_total, punter p2
        where  punter_punter_id = p2.punter_id and
                ca_time_day_id = this_week_last_day_id) and
    kontotyp = 'PERSON' and status = 'Aktivt' and
    startdate <= to_char(short_week,'yyyymmdd')
   group by punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1002,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1003,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

-- 1003
  insert into customer_cycle(
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    ca_time_day_id, punter_punter_id, CY_ID_1003 as ca_cycle_id
  from num_of_game_days_total
  where
    ca_time_day_id = this_week_last_day_id and
    NUM_OF_GAME_DAYS_R13_WEEK = 0 and NUM_OF_GAME_DAYS_R52_WEEK > 0 and
    punter_punter_id not in (select punter_punter_id from customer_cycle
                  where ca_time_day_id = this_week_last_day_id);

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1003,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1004,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

-- 1004
  insert into customer_cycle(
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    ca_time_day_id, punter_punter_id, CY_ID_1004 as ca_cycle_id
  from num_of_game_days_total
  where
    ca_time_day_id = this_week_last_day_id and
    NUM_OF_GAME_DAYS_R52_WEEK = 0 and
    punter_punter_id not in (select punter_punter_id from customer_cycle
                  where ca_time_day_id = this_week_last_day_id);

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1004,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1005,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

-- 1005
  insert into customer_cycle(
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    ca_time_day_id, punter_punter_id, CY_ID_1005 as ca_cycle_id
  from num_of_game_days_total, ca_cycle
  where
    ca_cycle.id = CY_ID_1005 and
    ca_time_day_id = this_week_last_day_id and
    NUM_OF_GAME_DAYS_R13_WEEK != 0 and NUM_OF_GAME_DAYS_R52_WEEK != 0 and
    (NUM_OF_GAME_DAYS_R13_WEEK - (NUM_OF_GAME_DAYS_R52_WEEK / 4)) / (NUM_OF_GAME_DAYS_R52_WEEK / 4) < (asc_desc_percentage * -1)
    and punter_punter_id not in (select punter_punter_id from customer_cycle
                  where ca_time_day_id = this_week_last_day_id);


    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1005,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1006,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

-- 1006
  insert into customer_cycle(
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    ca_time_day_id, punter_punter_id, CY_ID_1006 as ca_cycle_id
  from num_of_game_days_total, ca_cycle
  where
    ca_cycle.id = CY_ID_1006 and
    ca_time_day_id = this_week_last_day_id and
    NUM_OF_GAME_DAYS_R13_WEEK != 0 and NUM_OF_GAME_DAYS_R52_WEEK != 0 and
    (NUM_OF_GAME_DAYS_R13_WEEK - (NUM_OF_GAME_DAYS_R52_WEEK / 4)) / (NUM_OF_GAME_DAYS_R52_WEEK / 4) > asc_desc_percentage
    and punter_punter_id not in (select punter_punter_id from customer_cycle
                  where ca_time_day_id = this_week_last_day_id);



    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1006,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1007,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);

-- 1007
  insert into customer_cycle(
      ca_time_day_id, punter_punter_id, ca_cycle_id)
  select
    ca_time_day_id, punter_punter_id, CY_ID_1007 as ca_cycle_id
  from num_of_game_days_total
  where
    ca_time_day_id = this_week_last_day_id and
    punter_punter_id not in (select punter_punter_id from customer_cycle
                  where ca_time_day_id = this_week_last_day_id);


    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1007,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_cycle;

------------------------------------------------------------------------------


procedure load_customer_luck_skill(p_date date) is --replaced by load_customer_bet_mode - keep the table customer_luck_skill for now.

  last_day_of_week_indicator pls_integer;
  this_week_last_day_id      pls_integer;
  row_count                  pls_integer;

  this_week                  pls_integer;

  CLS_ID_1001     constant  pls_integer := 1001;
  CLS_ID_1002     constant  pls_integer := 1002;
  BM_ID_1001      constant  pls_integer := 1001;

  MODULE_NAME            constant varchar2(30) := 'load_customer_luck_skill';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from customer_luck_skill
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

-- r13, r52 and r_tot
  insert into customer_luck_skill (ca_time_day_id, punter_punter_id, ca_luck_skill_id)
  with nogd_tot as (
          select num_of_game_days_total.ca_time_day_id,
                 num_of_game_days_total.num_of_game_days_r13_week,
                 num_of_game_days_total.num_of_game_days_r52_week,
                 num_of_game_days_total.num_of_game_days_total as num_of_game_days_tot,
                 punter.punter_key,
                 max(num_of_game_days_total.punter_punter_id) punter_punter_id
          from num_of_game_days_total, punter
          where num_of_game_days_total.punter_punter_id = punter.punter_id and
                punter.kontotyp=TYPE_OF_ACCOUNT and
                num_of_game_days_total.ca_time_day_id = this_week_last_day_id
          group by num_of_game_days_total.ca_time_day_id,
                   num_of_game_days_total.num_of_game_days_r13_week,
                   num_of_game_days_total.num_of_game_days_r52_week,
                   num_of_game_days_total.num_of_game_days_total,
                   punter.punter_key
                    ),
    rev as (
          select customer_revenue.ca_time_day_id,
                 customer_revenue.ca_bet_method_group_id,
                 punter.punter_key,
                 sum(customer_revenue.revenue_r13_week) as revenue_r13_week,
                 sum(customer_revenue.revenue_r52_week) as revenue_r52_week,
                 sum(customer_revenue.revenue_total) as revenue_total
          from customer_revenue, punter
          where customer_revenue.punter_punter_id = punter.punter_id and
                punter.kontotyp=TYPE_OF_ACCOUNT and
                customer_revenue.ca_time_day_id = this_week_last_day_id
          group by customer_revenue.ca_time_day_id,
                   customer_revenue.ca_bet_method_group_id,
                   punter.punter_key
           )
     select nogd_tot.ca_time_day_id, nogd_tot.punter_punter_id,
            case when num_of_game_days_r13_week>=TEN then
                (case when
                 coalesce(sum(case when rev.ca_bet_method_group_id =  BM_ID_1001 then rev.revenue_r13_week end),ZERO) >=
                    ((coalesce(sum(case when rev.ca_bet_method_group_id =  BM_ID_1001 then rev.revenue_r13_week end),ZERO) +
                      coalesce(sum(case when rev.ca_bet_method_group_id != BM_ID_1001 then rev.revenue_r13_week end),ZERO)) / TWO) then
                  CLS_ID_1001
                else
                  CLS_ID_1002
                end)
            when num_of_game_days_r52_week>=TEN then
                (case when
                 coalesce(sum(case when rev.ca_bet_method_group_id =  BM_ID_1001 then rev.revenue_r52_week end),ZERO) >=
                    ((coalesce(sum(case when rev.ca_bet_method_group_id =  BM_ID_1001 then rev.revenue_r52_week end),ZERO) +
                      coalesce(sum(case when rev.ca_bet_method_group_id != BM_ID_1001 then rev.revenue_r52_week end),ZERO)) / TWO) then
                  CLS_ID_1001
                else
                  CLS_ID_1002
                end)
            else
                (case when
                  coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_total end),ZERO) >=
                    ((coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_total end),ZERO) +
                      coalesce(sum(case when ca_bet_method_group_id != BM_ID_1001 then revenue_total end),ZERO)) / TWO) then
                  CLS_ID_1001
                else
                  CLS_ID_1002
                end)
            end       as ca_luck_skill_id
      from rev inner join nogd_tot
        on rev.ca_time_day_id=nogd_tot.ca_time_day_id and
           rev.punter_key=nogd_tot.punter_key
     group by nogd_tot.ca_time_day_id, nogd_tot.punter_punter_id,num_of_game_days_r13_week, num_of_game_days_r52_week;

            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);
                                     
/*
  select num_of_game_days_total.ca_time_day_id, num_of_game_days_total.punter_punter_id,
    case when
      coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_r13_week end),ZERO) >=
        ((coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_r13_week end),ZERO) +
          coalesce(sum(case when ca_bet_method_group_id != BM_ID_1001 then revenue_r13_week end),ZERO)) / TWO) then
      CLS_ID_1001
    else
      CLS_ID_1002
    end                             as ca_luck_skill_id
  from num_of_game_days_total, customer_revenue
  where num_of_game_days_total.punter_punter_id = customer_revenue.punter_punter_id and
      num_of_game_days_total.ca_time_day_id = customer_revenue.ca_time_day_id and
      num_of_game_days_total.num_of_game_days_r13_week >= TEN and
      num_of_game_days_total.ca_time_day_id = this_week_last_day_id
  group by num_of_game_days_total.ca_time_day_id, num_of_game_days_total.punter_punter_id;

-- r52
  insert into customer_luck_skill (ca_time_day_id, punter_punter_id, ca_luck_skill_id)
  select num_of_game_days_total.ca_time_day_id, num_of_game_days_total.punter_punter_id,
    case when
      coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_r52_week end),ZERO) >=
        ((coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_r52_week end),ZERO) +
          coalesce(sum(case when ca_bet_method_group_id != BM_ID_1001 then revenue_r52_week end),ZERO)) / TWO) then
      CLS_ID_1001
    else
      CLS_ID_1002
    end                             as ca_luck_skill_id
  from num_of_game_days_total, customer_revenue
  where num_of_game_days_total.punter_punter_id = customer_revenue.punter_punter_id and
      num_of_game_days_total.ca_time_day_id = customer_revenue.ca_time_day_id and
      num_of_game_days_total.num_of_game_days_r52_week >= TEN and
      num_of_game_days_total.ca_time_day_id = this_week_last_day_id and
      num_of_game_days_total.punter_punter_id not in (select punter_punter_id from customer_luck_skill where ca_time_day_id=this_week_last_day_id)
  group by num_of_game_days_total.ca_time_day_id, num_of_game_days_total.punter_punter_id;

-- total
  insert into customer_luck_skill (ca_time_day_id, punter_punter_id, ca_luck_skill_id)
  select num_of_game_days_total.ca_time_day_id, num_of_game_days_total.punter_punter_id,
    case when
      coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_total end),ZERO) >=
        ((coalesce(sum(case when ca_bet_method_group_id =  BM_ID_1001 then revenue_total end),ZERO) +
          coalesce(sum(case when ca_bet_method_group_id != BM_ID_1001 then revenue_total end),ZERO)) / TWO) then
      CLS_ID_1001
    else
      CLS_ID_1002
    end                             as ca_luck_skill_id
  from num_of_game_days_total, customer_revenue
  where num_of_game_days_total.punter_punter_id = customer_revenue.punter_punter_id and
      num_of_game_days_total.ca_time_day_id = customer_revenue.ca_time_day_id and
      num_of_game_days_total.ca_time_day_id = this_week_last_day_id and
      num_of_game_days_total.punter_punter_id not in (select punter_punter_id from customer_luck_skill where ca_time_day_id=this_week_last_day_id)
  group by num_of_game_days_total.ca_time_day_id, num_of_game_days_total.punter_punter_id;
*/
exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_luck_skill;

------------------------------------------------------------------------------

procedure load_customer_gallop_and_vr(p_date date) is

  last_day_of_week_indicator pls_integer;
  this_week_last_day_id      pls_integer;
  row_count                  pls_integer;

  this_week                  pls_integer;

  CGV_ID_1001     constant  pls_integer := 1001;
  CGV_ID_1002     constant  pls_integer := 1002;

  CA_RACE_TYPE_ID_1002     constant  pls_integer := 1002;
  CA_GAME_TYPE_ID_1002     constant  pls_integer := 1002;

  MODULE_NAME            constant varchar2(30) := 'load_customer_gallop_and_vr';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from customer_gallop_and_vr
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

  insert into customer_gallop_and_vr(
      ca_time_day_id, punter_punter_id, ca_gallop_and_vr_id)
  select
    ca_time_day_id, punter_punter_id, CGV_ID_1001 as ca_gallop_and_vr_id
  from num_of_game_days_hr_race_type
  where
    ca_time_day_id = this_week_last_day_id and
    ca_race_type_id = CA_RACE_TYPE_ID_1002 and NUM_OF_GAME_DAYS_R52_WEEK >= TWO;
    
    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 2,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);    

  insert into customer_gallop_and_vr(
      ca_time_day_id, punter_punter_id, ca_gallop_and_vr_id)
  select
    ca_time_day_id, punter_punter_id, CGV_ID_1002 as ca_gallop_and_vr_id
  from num_of_game_days_game_type
  where
    ca_time_day_id = this_week_last_day_id and
    ca_game_type_id = CA_GAME_TYPE_ID_1002 and NUM_OF_GAME_DAYS_R52_WEEK >= TWO;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 2,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_gallop_and_vr;

------------------------------------------------------------------------------

procedure load_customer_channel(p_date date) is

  last_day_of_week_indicator pls_integer;
  this_week_last_day_id      pls_integer;
  row_count                  pls_integer;

  this_week                  pls_integer;

  CCH_ID_1001     constant  pls_integer := 1001;
  CCH_ID_1002     constant  pls_integer := 1002;
  CCH_ID_1003     constant  pls_integer := 1003;
  CCH_ID_1004     constant  pls_integer := 1004;

  E_CHANNEL_ID    constant  pls_integer := 2409;

  MODULE_NAME            constant varchar2(30) := 'load_customer_channel';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from customer_channel
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

     ld_start_time:=sysdate;
    
  
     utilities.reg_batch_log( pin_package => JOB_NAME,
                              pin_procedure => MODULE_NAME,
                              pin_started_on => ld_start_time,
                              pin_step_in_proc => 1,
                              pin_no_rows => null,
                              pio_id => ln_batch_log_id,
                              pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

-- num_of_game_days_r52_week
  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, punter_key)
  select ca_time_day_id, max(punter_punter_id), CCH_ID_1001 as ca_channel_id, punter_key
  from NUM_OF_GAME_DAYS_CHANNEL_REV a
  where not exists
      (select null from NUM_OF_GAME_DAYS_CHANNEL_REV
          where punter_key = a.punter_key and
            num_of_game_days_r52_week > ZERO and
            channel_channel_id != E_CHANNEL_ID and
            ca_time_day_id = a.ca_time_day_id) and
      num_of_game_days_r52_week > ZERO and
      channel_channel_id = E_CHANNEL_ID and
      ca_time_day_id = this_week_last_day_id
  GROUP BY ca_time_day_id, punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;

---------------------------------------------------------------
    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 2,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  


  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, punter_key)
  select distinct ca_time_day_id, max(punter_punter_id), CCH_ID_1002 as ca_channel_id, punter_key
  from NUM_OF_GAME_DAYS_CHANNEL_REV a
  where not exists
      (select null from NUM_OF_GAME_DAYS_CHANNEL_REV
          where punter_key = a.punter_key and
            num_of_game_days_r52_week > ZERO and
            channel_channel_id = E_CHANNEL_ID and
            ca_time_day_id = a.ca_time_day_id) and
      num_of_game_days_r52_week > ZERO and
      channel_channel_id != E_CHANNEL_ID and
      ca_time_day_id = this_week_last_day_id
  GROUP BY ca_time_day_id, punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 2,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 3,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  


  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, punter_key)
  select ca_time_day_id, max(punter_punter_id), CCH_ID_1003 as ca_channel_id, punter_key
  from NUM_OF_GAME_DAYS_CHANNEL_REV a
  where exists
      (select null from NUM_OF_GAME_DAYS_CHANNEL_REV
          where punter_key = a.punter_key and
            num_of_game_days_r52_week > ZERO and
            channel_channel_id != E_CHANNEL_ID and
            ca_time_day_id = a.ca_time_day_id) and
      num_of_game_days_r52_week > ZERO and
      channel_channel_id = E_CHANNEL_ID and
      ca_time_day_id = this_week_last_day_id
  GROUP BY ca_time_day_id, punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 3,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 4,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  


-- total
  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, punter_key)
  select ca_time_day_id, max(punter_punter_id), CCH_ID_1001 as ca_channel_id, punter_key
  from NUM_OF_GAME_DAYS_CHANNEL_REV a
  where not exists
      (select null from NUM_OF_GAME_DAYS_CHANNEL_REV
          where punter_key = a.punter_key and
            channel_channel_id != E_CHANNEL_ID and
            ca_time_day_id = a.ca_time_day_id) and
      channel_channel_id = E_CHANNEL_ID and
      ca_time_day_id = this_week_last_day_id and
      punter_key not in (select c.punter_key from customer_channel c where ca_time_day_id = this_week_last_day_id)
  GROUP BY ca_time_day_id, punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 4,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 5,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  

  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, punter_key)
  select distinct ca_time_day_id, max(punter_punter_id), CCH_ID_1002 as ca_channel_id, punter_key
  from NUM_OF_GAME_DAYS_CHANNEL_REV a
  where not exists
      (select null from NUM_OF_GAME_DAYS_CHANNEL_REV
          where punter_key = a.punter_key and
            channel_channel_id = E_CHANNEL_ID and
            ca_time_day_id = a.ca_time_day_id) and
      channel_channel_id != E_CHANNEL_ID and
      ca_time_day_id = this_week_last_day_id and
      punter_key not in (select c.punter_key from customer_channel c where ca_time_day_id = this_week_last_day_id)
  GROUP BY ca_time_day_id, punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 5,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 6,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  

  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, punter_key)
  select ca_time_day_id, max(punter_punter_id), CCH_ID_1003 as ca_channel_id, punter_key
  from NUM_OF_GAME_DAYS_CHANNEL_REV a
  where exists
      (select null from NUM_OF_GAME_DAYS_CHANNEL_REV
          where punter_key = a.punter_key and
            channel_channel_id != E_CHANNEL_ID and
            ca_time_day_id = a.ca_time_day_id) and
      channel_channel_id = E_CHANNEL_ID and
      ca_time_day_id = this_week_last_day_id and
      punter_key not in (select c.punter_key from customer_channel c where ca_time_day_id = this_week_last_day_id)
  GROUP BY ca_time_day_id, punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 6,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 7,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  

-- home store, r52_week
  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, channel_channel_id, partner_channel_id, punter_key)
  select max(a.ca_time_day_id) as ca_time_day_id, max(a.punter_punter_id), CCH_ID_1004 as ca_channel_id,
          min(a.channel_channel_id) as channel_channel_id,
          min(a.partner_channel_id) as partner_channel_id, a.punter_key
  from NUM_OF_GAME_DAYS_CHANNEL_REV a,
    (select punter_key, max(num_of_game_days_r52_week) as max_nogd
      from NUM_OF_GAME_DAYS_CHANNEL_REV
      where channel_channel_id != E_CHANNEL_ID
      and ca_time_day_id = this_week_last_day_id
      group by punter_key) b
  where a.punter_key = b.punter_key
  and a.channel_channel_id != E_CHANNEL_ID
  and a.ca_time_day_id = this_week_last_day_id
  and a.num_of_game_days_r52_week = b.max_nogd
  group by a.punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 7,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
 

/*
-- home store, total
  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, channel_channel_id, partner_channel_id)
  select max(a.ca_time_day_id) as ca_time_day_id, a.punter_punter_id, CCH_ID_1004 as ca_channel_id,
          min(a.channel_channel_id) as channel_channel_id,
          min(a.partner_channel_id) as partner_channel_id
  from NUM_OF_GAME_DAYS_CHANNEL_REV a,
    (select punter_punter_id, max(num_of_game_days_total) as max_nogd
      from NUM_OF_GAME_DAYS_CHANNEL_REV
      where channel_channel_id != E_CHANNEL_ID
      and ca_time_day_id = this_week_last_day_id
      group by punter_punter_id) b
  where a.punter_punter_id = b.punter_punter_id
  and a.channel_channel_id != E_CHANNEL_ID
  and a.ca_time_day_id = this_week_last_day_id
  and a.num_of_game_days_total = b.max_nogd
  and a.punter_punter_id not in
    (select punter_punter_id from customer_channel
        where ca_channel_id = CCH_ID_1004 and
              ca_time_day_id = this_week_last_day_id)
  group by a.punter_punter_id;
*/

    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 8,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id); 

-- home store, recruit
  insert into customer_channel(
      ca_time_day_id, punter_punter_id, ca_channel_id, channel_channel_id, punter_key)
  select this_week_last_day_id as ca_time_day_id, min(pur_punter_id) as punter_punter_id,
    CCH_ID_1004 as ca_channel_id, min(chl_channel_id) as channel_channel_id, punter_key
  from acc_event_fact aef, punter p
  where  p.punter_id=aef.pur_punter_id
    and  p.punter_key in (select c.punter_key from customer_channel c
                              where ca_channel_id != CCH_ID_1004 and
                                ca_time_day_id = this_week_last_day_id) and
    aee_id = ONE and
    chl_channel_id != E_CHANNEL_ID and
    p.punter_key not in
    (select c.punter_key from customer_channel c
        where ca_channel_id = CCH_ID_1004 and
              ca_time_day_id = this_week_last_day_id)
  group by p.punter_key;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 8,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_channel;


-----------------------------------------------------------------------------

procedure load_customer_together(p_date date) is

   last_day_of_week_indicator pls_integer;
   this_week_last_day_id      pls_integer;
   row_count                  pls_integer;

   this_week                  pls_integer;

  MODULE_NAME            constant varchar2(30) := 'load_customer_together';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  C_STATUS                 constant varchar2(20) := 'Aktivt';
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
begin

  select is_last_day_of_week, id, year_week
    into last_day_of_week_indicator, this_week_last_day_id, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = ONE then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
  from customer_together
  where ca_time_day_id = this_week_last_day_id
  and rownum < TWO;

  if row_count = ZERO then null; else return; end if;

    ld_start_time:=sysdate;
    
  
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));

        -- Kategori tillsammansspelare som någon gång har spelat
        insert into customer_together(CA_TIME_DAY_ID, PUNTER_PUNTER_ID, CA_TOGETHER_ID)
        select ca_time_day_id, punter_punter_id, case when nogdt.nogd_captain_r13_week > 0 then
                        1001 --'Aktiv lagkapten'
                    when (nogdt.nogd_captain_r13_week = 0 and nogdt.nogd_member_r13_week > 0) then
                        1003 --'Aktiv lagmedlem'
                    when (select count(lagledare_id) from mdb.tillsammans_lag tl, spst.punter p where tl.lagledare_id=p.acc_index and nogdt.punter_punter_id=p.punter_id) > 0 and nogdt.nogd_captain_r13_week = 0 then
                        1002 --'Inaktiv lagkapten'
                    when (nogdt.nogd_captain_r13_week = 0 and nogdt.nogd_member_r13_week = 0) then
                        1004 --'Inaktiv lagmedlem'
                    end as ca_together_id
          from num_of_game_days_together nogdt
         where ca_time_day_id=this_week_last_day_id;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 2,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  


         -- Inaktiv lagkapten tillsammansspelare som ALDRIG har spelat
         insert into customer_together(ca_time_day_id, punter_punter_id, ca_together_id)
         select this_week_last_day_id, pc.punter_id, 1002
           from mdb.tillsammans_lag tl inner join punter_current pc
             on pc.acc_index=tl.lagledare_id
            and tl.status=C_STATUS --Aktivt
            and pc.status=C_STATUS --Aktivt
            and pc.kontotyp=TYPE_OF_ACCOUNT
            and not exists (select 1
                              from customer_together ct inner join punter p
                                 on p.punter_id=ct.punter_punter_id
                                and p.acc_index=tl.lagledare_id
                                and ca_time_day_id=this_week_last_day_id)
          group by pc.punter_id;


    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 2,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);
                             
    ln_batch_log_id:=null;
    
    ---------------------------------------------------------------
    ld_start_time:=sysdate;
    
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 3,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);  

         -- Inaktiv lagmedlem tillsammansspelare som ALDRIG har spelat
         insert into customer_together(ca_time_day_id, punter_punter_id, ca_together_id)
         select this_week_last_day_id, pc.punter_id, 1004
           from mdb.lagmedlemmar lm inner join punter_current pc
             on pc.acc_index=lm.konto_kontoindex
            and pc.kontotyp=TYPE_OF_ACCOUNT
            and pc.status=C_STATUS --Aktivt
            and slutdatum > p_date
            and not exists (select 1
                             from customer_together ct inner join punter p
                                  on p.punter_id=ct.punter_punter_id
                                  and p.acc_index=lm.konto_kontoindex
                                  and ca_time_day_id=this_week_last_day_id)
          group by pc.punter_id;

    l_num_of_rows:=sql%rowcount;        
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 3,
                             pin_no_rows => l_num_of_rows,
                             pio_id => ln_batch_log_id);

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_together;

------------------------------------------------------------------------------

procedure load_customer_shared_bet_type(p_date date) is

  last_day_of_week_indicator    pls_integer;
  this_week_last_day_id         pls_integer;
  row_count                     pls_integer;

  ld_start_time date;
  ln_batch_log_id               batch_log.id%type;
  l_num_of_rows                 number;
  l_r_short_prg_date            varchar2(30);
  l_r_long_prg_date             varchar2(30);
  
  
  /* From table shared_bet_type
     NOT_SHARED_BET	        Ej andelsspel
  	 SHARED_BET_ORIGINAL	Tillsammans  	 
  	 SHARED_BET_RETAIL	    Butiksandelar
  	 SHARED_BET_FRIENDS	    Privata andelar */  
  ln_shared_bet_not_shared_bet  pls_integer;
  ln_shared_bet_together_id     pls_integer;
  ln_shared_bet_retail_id       pls_integer;
  ln_shared_bet_friends_id      pls_integer;
  
  TME_TIME_ID_START             constant varchar2(30) := '2018120100';
  PRG_DATE_START                constant varchar2(30) := '20181230';  -- Start from week one 2019 (week one 2019 start 20181231)
  MODULE_NAME                   constant varchar2(30) := 'load_customer_shared_bet_type';  
  --JOB_MODULE                    constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
 
begin

  select is_last_day_of_week, id --, year_week
    into last_day_of_week_indicator, this_week_last_day_id --, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = 1 then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
    from customer_shared_bet_type
   where ca_time_day_id = this_week_last_day_id
     and rownum < 2; -- Om det finns en rad!

  -- If we already have data for a week do nothing more.
  if row_count = 0 then null; else return; end if;

  -- ROLLING_SHORT_WEEK
  select to_char(full_date,'yyyymmdd') into l_r_short_prg_date from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;  
  -- ROLLING_LONG_WEEK
  select to_char(full_date,'yyyymmdd') into l_r_long_prg_date from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
  
  -- SHARED_BET_ORIGINAL: Tillsammans
  select id 
    into ln_shared_bet_together_id 
    from shared_bet_type 
   where code='SHARED_BET_ORIGINAL';
     
  -- SHARED_BET_RETAIL:   Butiksandelar
  select id 
    into ln_shared_bet_retail_id 
    from shared_bet_type 
   where code='SHARED_BET_RETAIL';
     
  -- SHARED_BET_FRIENDS:  Privata andelar
  select id 
    into ln_shared_bet_friends_id 
    from shared_bet_type 
   where code='SHARED_BET_FRIENDS';
    
  --NOT_SHARED_BET:       Ej andelsspel
  select id 
    into ln_shared_bet_not_shared_bet 
    from shared_bet_type 
   where code='NOT_SHARED_BET';

  ld_start_time:=sysdate;
    

  utilities.reg_batch_log( pin_package => JOB_NAME,
                           pin_procedure => MODULE_NAME,
                           pin_started_on => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows => null,
                           pio_id => ln_batch_log_id,
                           pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));


  insert into customer_shared_bet_type (ca_time_day_id, punter_key, shared_bet_type_id)
  with shared_bets as 
      (select p.punter_key,
             count(distinct case when prg.prg_date > l_r_short_prg_date 
                            then prg_date 
                            end) num_of_game_days_13w,
             count(distinct case when prg.prg_date > l_r_long_prg_date  
                            then prg_date 
                            end) num_of_game_days_52w,
             sum(case when (prg.prg_date > l_r_short_prg_date and b.shared_bet_type_id=ln_shared_bet_together_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_together_13w,
             sum(case when (prg.prg_date > l_r_long_prg_date and b.shared_bet_type_id=ln_shared_bet_together_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_together_52w,
             sum(case when b.shared_bet_type_id=ln_shared_bet_together_id 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_together_tot,
             sum(case when (prg.prg_date > l_r_short_prg_date and b.shared_bet_type_id=ln_shared_bet_retail_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_retail_13w,
             sum(case when (prg.prg_date > l_r_long_prg_date and b.shared_bet_type_id=ln_shared_bet_retail_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_retail_52w,
             sum(case when b.shared_bet_type_id=ln_shared_bet_retail_id 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_retail_tot,
             sum(case when (prg.prg_date > l_r_short_prg_date and b.shared_bet_type_id=ln_shared_bet_friends_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_friends_13w,
             sum(case when (prg.prg_date > l_r_long_prg_date and b.shared_bet_type_id=ln_shared_bet_friends_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_friends_52w,
             sum(case when b.shared_bet_type_id=ln_shared_bet_friends_id 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_friends_tot
        from betfact b
             inner join punter p on (p.punter_id=b.pun_punter_id)
             inner join program prg on (prg.program_id=b.prg_program_id)
          where b.tme_time_id > TME_TIME_ID_START
            and prg.prg_date > PRG_DATE_START
            and prg.prg_date <= to_char(p_date,'yyyymmdd') -- If we generate historical segments
            and kontotyp=TYPE_OF_ACCOUNT
            and p.punter_id > 0 -- Exclude anonymous customers
            and b.shared_bet_type_id != ln_shared_bet_not_shared_bet -- Not a shared bet
          group by p.punter_key)
        select this_week_last_day_id as ca_time_day_id,
               punter_key,
               case when num_of_game_days_13w >= 10 then
                        case when (bet_amount_together_13w >= bet_amount_retail_13w and bet_amount_together_13w >= bet_amount_friends_13w) then
                                ln_shared_bet_together_id --'Tillsammans'
                            when (bet_amount_retail_13w >= bet_amount_friends_13w) then
                                ln_shared_bet_retail_id --'Butiksandelar'
                            when (bet_amount_friends_13w > 0) then
                                ln_shared_bet_friends_id --'Privata andelar'
                        end
                    when num_of_game_days_52w >= 10 then
                        case when (bet_amount_together_52w >= bet_amount_retail_52w and bet_amount_together_52w >= bet_amount_friends_52w) then
                                ln_shared_bet_together_id --'Tillsammans'
                            when (bet_amount_retail_52w >= bet_amount_friends_52w) then
                                ln_shared_bet_retail_id   --'Butiksandelar'
                            when (bet_amount_friends_52w > 0) then
                                ln_shared_bet_friends_id  --'Privata andelar'
                        end
               else
                    case when (bet_amount_together_tot >= bet_amount_retail_tot and bet_amount_together_tot >= bet_amount_friends_tot) then
                            ln_shared_bet_together_id --'Tillsammans'
                         when (bet_amount_retail_tot >= bet_amount_friends_tot) then
                            ln_shared_bet_retail_id --'Butiksandelar'
                         when (bet_amount_friends_tot > 0) then
                            ln_shared_bet_friends_id --'Privata andelar'
                    end
               end shared_bet_type_id --,
               --num_of_game_days_13w,
               --num_of_game_days_52w,
               --bet_amount_together_13w,
               --bet_amount_together_52w,
               --bet_amount_together_tot,
               --bet_amount_retail_13w,
               --bet_amount_retail_52w,
               --bet_amount_retail_tot,
               --bet_amount_friends_13w,
               --bet_amount_friends_52w,
               --bet_amount_friends_tot
          from shared_bets;  



  l_num_of_rows:=sql%rowcount;
  --dbms_output.put_line('l_num_of_rows: '||l_num_of_rows);
  --dbms_output.put_line('ld_start_time: '||ld_start_time);
  --dbms_output.put_line('end_time: '||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'));
  --dbms_output.put_line('l_r_short_prg_date: '||l_r_short_prg_date);
  --dbms_output.put_line('l_r_long_prg_date: '||l_r_long_prg_date);        
        
            
  utilities.reg_batch_log( pin_package => JOB_NAME,
                           pin_procedure => MODULE_NAME,
                           pin_started_on => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows => l_num_of_rows,
                           pio_id => ln_batch_log_id);
                                     

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;

end load_customer_shared_bet_type;


------------------------------------------------------------------------------

procedure deployment_init_shared_bet_type is

  l_mindate date := to_date('20200105','yyyymmdd');
  l_mindate_td date;
  max_date date;
  
  TME_TIME_ID_START             constant varchar2(30) := '2018120100';

begin

  select min(full_date) into l_mindate_td from ca_time_day;

  if l_mindate_td>l_mindate then l_mindate:=l_mindate_td; end if;

  select max(to_date(prg_date,'yyyymmdd')) 
    into max_date 
    from program prg
        inner join betfact bf on (prg.program_id=bf.prg_program_id)
   where prg_date >= to_char(l_mindate,'yyyymmdd')
     and prg_date != 'Saknas'
     and tme_time_id >= TME_TIME_ID_START;

  while l_mindate<=max_date loop
    load_customer_shared_bet_type(l_mindate);
    l_mindate:=l_mindate+1;
    commit;
  end loop;

end deployment_init_shared_bet_type;

------------------------------------------------------------------------------

procedure load_customer_bet_mode(p_date date) is

  last_day_of_week_indicator    pls_integer;
  this_week_last_day_id         pls_integer;
  row_count                     pls_integer;

  ld_start_time date;
  ln_batch_log_id               batch_log.id%type;
  l_num_of_rows                 number;
  l_r_short_prg_date            varchar2(30);
  l_r_long_prg_date             varchar2(30);
  
  
  /* From table bet_mode
     MEMBER_BET	Vanlig
     SHARED_BET	Andel
     HARRY_BET	Harry Boy
 */  
  
  ln_member_bet_id     pls_integer;
  ln_shared_bet_id     pls_integer;
  ln_harry_bet_id      pls_integer;
  
  ln_skill_id          pls_integer;
  ln_luck_id           pls_integer;
  ln_share_id          pls_integer;
    
  
  TME_TIME_ID_START             constant varchar2(30) := '2018120100';
  PRG_DATE_START                constant varchar2(30) := '20181230';  -- Start from week one 2019 (week one 2019 start 20181231)
  MODULE_NAME                   constant varchar2(30) := 'load_customer_bet_mode';  
 
begin

  select is_last_day_of_week, id --, year_week
    into last_day_of_week_indicator, this_week_last_day_id --, this_week
    from ca_time_day
  where full_date = p_date;

  if last_day_of_week_indicator = 1 then null; else return; end if;

  select /*+ first_rows(1) */ count(distinct ca_time_day_id)
    into row_count
    from customer_bet_mode
   where ca_time_day_id = this_week_last_day_id
     and rownum < 2; -- Om det finns en rad!

  -- If we already have data for a week do nothing more.
  if row_count = 0 then null; else return; end if;

  -- ROLLING_SHORT_WEEK
  select to_char(full_date,'yyyymmdd') into l_r_short_prg_date from ca_time_day a where full_date=p_date-7*ROLLING_SHORT_WEEK;  
  -- ROLLING_LONG_WEEK
  select to_char(full_date,'yyyymmdd') into l_r_long_prg_date from ca_time_day a where full_date=p_date-7*ROLLING_LONG_WEEK;
  
  -- MEMBER_BET: Vanlig
  select id 
    into ln_member_bet_id 
    from bet_mode 
   where code='MEMBER_BET';
     
  -- SHARED_BET:   Andel
  select id 
    into ln_shared_bet_id 
    from bet_mode 
   where code='SHARED_BET';
     
  -- HARRY_BET:  Harry boy
  select id 
    into ln_harry_bet_id 
    from bet_mode 
   where code='HARRY_BET';
  
  select id 
    into ln_luck_id 
    from ca_luck_skill 
   where luck_skill_name='Turspelare';
     
  select id 
    into ln_skill_id 
    from ca_luck_skill 
   where luck_skill_name='Skicklighetsspelare';     
   
  select id 
    into ln_share_id 
    from ca_luck_skill 
   where luck_skill_name='Andelsspelare';   
    
  ld_start_time:=sysdate;

  utilities.reg_batch_log( pin_package => JOB_NAME,
                           pin_procedure => MODULE_NAME,
                           pin_started_on => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows => null,
                           pio_id => ln_batch_log_id,
                           pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));


  insert into customer_bet_mode (ca_time_day_id, punter_key, bet_mode_id)
  with bet_mode_weeks as 
      (select p.punter_key,
             count(distinct case when prg.prg_date > l_r_short_prg_date 
                            then prg_date 
                            end) num_of_game_days_13w,
             count(distinct case when prg.prg_date > l_r_long_prg_date  
                            then prg_date 
                            end) num_of_game_days_52w,
             sum(case when (prg.prg_date > l_r_short_prg_date and b.bet_mode_id=ln_member_bet_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_member_bet_13w,
             sum(case when (prg.prg_date > l_r_long_prg_date and b.bet_mode_id=ln_member_bet_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_member_bet_52w,
             sum(case when b.bet_mode_id=ln_member_bet_id 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_member_bet_tot,
             sum(case when (prg.prg_date > l_r_short_prg_date and b.bet_mode_id=ln_shared_bet_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_shared_bet_13w,
             sum(case when (prg.prg_date > l_r_long_prg_date and b.bet_mode_id=ln_shared_bet_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_shared_bet_52w,
             sum(case when b.bet_mode_id=ln_shared_bet_id 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_shared_bet_tot,
             sum(case when (prg.prg_date > l_r_short_prg_date and b.bet_mode_id=ln_harry_bet_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_harry_bet_13w,
             sum(case when (prg.prg_date > l_r_long_prg_date and b.bet_mode_id=ln_harry_bet_id) 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_harry_bet_52w,
             sum(case when b.bet_mode_id=ln_harry_bet_id 
                 then 
                    bet_amount 
                 else 0 
                 end) bet_amount_harry_bet_tot
        from betfact b
             inner join punter p on (p.punter_id=b.pun_punter_id)
             inner join program prg on (prg.program_id=b.prg_program_id)
          where b.tme_time_id > TME_TIME_ID_START
            and prg.prg_date > PRG_DATE_START 
            and kontotyp=TYPE_OF_ACCOUNT
            and p.punter_id > 0 -- Exclude anonymous customers
          group by p.punter_key)
        select this_week_last_day_id as ca_time_day_id,
               punter_key,
               case when num_of_game_days_13w >= 10 then
                        case when (bet_amount_member_bet_13w >= bet_amount_harry_bet_13w and bet_amount_member_bet_13w >= bet_amount_shared_bet_13w) then
                                ln_skill_id
                            when (bet_amount_harry_bet_13w >= bet_amount_shared_bet_13w) then
                                ln_luck_id
                            when (bet_amount_shared_bet_13w > 0) then
                                ln_share_id
                        end
                    when num_of_game_days_52w >= 10 then
                        case when (bet_amount_member_bet_52w >= bet_amount_harry_bet_52w and bet_amount_member_bet_52w >= bet_amount_shared_bet_52w) then
                                ln_skill_id
                            when (bet_amount_harry_bet_52w >= bet_amount_shared_bet_52w) then
                                ln_luck_id
                            when (bet_amount_shared_bet_52w > 0) then
                                ln_share_id
                        end
               else
                    case when (bet_amount_member_bet_tot >= bet_amount_harry_bet_tot and bet_amount_member_bet_tot >= bet_amount_shared_bet_tot) then
                            ln_skill_id
                         when (bet_amount_harry_bet_tot >= bet_amount_shared_bet_tot) then
                            ln_luck_id
                         when (bet_amount_shared_bet_tot > 0) then
                            ln_share_id
                    end
               end bet_mode_id --,
               --num_of_game_days_13w,
               --num_of_game_days_52w,
               --bet_amount_together_13w,
               --bet_amount_together_52w,
               --bet_amount_together_tot,
               --bet_amount_retail_13w,
               --bet_amount_retail_52w,
               --bet_amount_retail_tot,
               --bet_amount_friends_13w,
               --bet_amount_friends_52w,
               --bet_amount_friends_tot
          from bet_mode_weeks;  



  l_num_of_rows:=sql%rowcount;
  --dbms_output.put_line('l_num_of_rows: '||l_num_of_rows);
  --dbms_output.put_line('ld_start_time: '||ld_start_time);
  --dbms_output.put_line('end_time: '||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'));
  --dbms_output.put_line('l_r_short_prg_date: '||l_r_short_prg_date);
  --dbms_output.put_line('l_r_long_prg_date: '||l_r_long_prg_date);        
        
            
  utilities.reg_batch_log( pin_package => JOB_NAME,
                           pin_procedure => MODULE_NAME,
                           pin_started_on => ld_start_time,
                           pin_step_in_proc => 1,
                           pin_no_rows => l_num_of_rows,
                           pio_id => ln_batch_log_id);
                                     

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;

end load_customer_bet_mode;

procedure deployment_init_customer_bet_mode is

  l_mindate date := to_date('20200105','yyyymmdd');
  l_mindate_td date;
  max_date date;
  
  TME_TIME_ID_START             constant varchar2(30) := '2018120100';

begin

  select min(full_date) into l_mindate_td from ca_time_day;

  if l_mindate_td>l_mindate then l_mindate:=l_mindate_td; end if;

  select max(to_date(prg_date,'yyyymmdd')) 
    into max_date 
    from program prg
        inner join betfact bf on (prg.program_id=bf.prg_program_id)
   where prg_date >= to_char(l_mindate,'yyyymmdd')
     and prg_date != 'Saknas'
     and tme_time_id >= TME_TIME_ID_START;

  while l_mindate<=max_date loop
    load_customer_bet_mode(l_mindate);
    l_mindate:=l_mindate+1;
    commit;
  end loop;

end deployment_init_customer_bet_mode;

------------------------------------------------------------------------------

procedure load_customer_segment(p_date date) is

  MODULE_NAME            constant varchar2(30) := 'load_customer_segment';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

  last_day_of_week_indicator pls_integer;
  this_week_last_day_id      pls_integer;

  this_week                  pls_integer;

  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  
begin

        select is_last_day_of_week, id, year_week
          into last_day_of_week_indicator, this_week_last_day_id, this_week
          from ca_time_day
        where full_date = p_date;

        if last_day_of_week_indicator = ONE then null; else return; end if;

        execute immediate 'truncate table customer_segment';
        
        ld_start_time:=sysdate;
        
      
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => null,
                                 pio_id => ln_batch_log_id,
                                 pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));
        

        insert into customer_segment (year_week,punter_key,CLUSTER_ID,cluster_name,frequency_id,frequency_name,cycle_id,cycle_name,gallop_id,gallop_name,vr_id,vr_name,luck_skill_id,luck_skill_name,revenue_interval_id,revenue_interval_class, revenue_interval_from,revenue_interval_to,channel_type_id,home_channel_key,channel_type_name,together_id,together_name,home_partner_channel_key,shared_bet_type_id,shared_bet_type_name)
            with cust_cluster as (
                           select p.punter_key,
                                  cac.id as cluster_id,
                                  cac.CLUSTER_NAME,
                                  ctd.year_week
                                  from spst.customer_cluster cclu inner join spst.ca_time_day ctd
                                    on (cclu.ca_time_day_id=ctd.id)
                                 inner join spst.punter p
                                    on (cclu.punter_punter_id=p.punter_id)
                                 inner join spst.ca_cluster cac
                                    on (cclu.ca_cluster_id =cac.id)
                                  where p.kontotyp='PERSON'
                                    --and cac.id!=1008 --Inget kluster
                                    and cclu.ca_time_day_id=this_week_last_day_id
                        ),
                cust_freguency as (
                           select p.punter_key,
                                  cac.id as frequency_id,
                                  cac.FREQUENCY_NAME,
                                  ctd.year_week
                                  from spst.customer_frequency cf inner join spst.ca_time_day ctd
                                    on (cf.ca_time_day_id=ctd.id)
                                 inner join spst.punter p
                                    on (cf.punter_punter_id=p.punter_id)
                                 inner join spst.ca_frequency cac
                                    on (cf.ca_frequency_id =cac.id)
                                  where p.kontotyp='PERSON'
                                    and cf.ca_time_day_id=this_week_last_day_id
                            ),
                 cust_customer_cycle as (
                           select p.punter_key,
                                  cac.id as cycle_id,
                                  cac.cycle_name,
                                  ctd.year_week
                                  from spst.customer_cycle cc1 inner join spst.ca_time_day ctd
                                    on (cc1.ca_time_day_id=ctd.id)
                                 inner join spst.punter p
                                    on (cc1.punter_punter_id=p.punter_id)
                                 inner join spst.ca_cycle cac
                                    on (cc1.ca_cycle_id =cac.id)
                                  where p.kontotyp='PERSON'
                                    --and cac.id!=1002 -- Ej aktiverad
                                    and cc1.ca_time_day_id=this_week_last_day_id
                            ),
                 cust_gallop_and_vr as (
                           select p.punter_key,
                                  cac.id as gallop_and_vr_id,
                                  cac.gallop_and_vr_name,
                                  ctd.year_week
                                  from spst.customer_gallop_and_vr cgav inner join spst.ca_time_day ctd
                                    on (cgav.ca_time_day_id=ctd.id)
                                 inner join spst.punter p
                                    on (cgav.punter_punter_id=p.punter_id)
                                 inner join spst.ca_gallop_and_vr cac
                                    on (cgav.ca_gallop_and_vr_id =cac.id)
                                  where p.kontotyp='PERSON'
                                    and cgav.ca_time_day_id=this_week_last_day_id
                            ),
                 cust_luck_skill as (
                           select cbm.punter_key,
                                  cac.id as luck_skill_id,
                                  cac.luck_skill_name,
                                  ctd.year_week
                                  from spst.customer_bet_mode cbm 
                                  inner join spst.ca_time_day ctd on (cbm.ca_time_day_id=ctd.id)
                                  inner join spst.ca_luck_skill cac on (cbm.bet_mode_id =cac.id)
                                  where cbm.ca_time_day_id=this_week_last_day_id
                            ),    
                 cust_revenue_interval as (
                           select p.punter_key,
                                  cac.id as revenue_interval_id,
                                  cac.class,
                                  cac.REVENUE_FROM,
                                  cac.REVENUE_TO,
                                  ctd.year_week
                                  from spst.customer_revenue_interval crv inner join spst.ca_time_day ctd
                                    on (crv.ca_time_day_id=ctd.id)
                                 inner join spst.punter p
                                    on (crv.punter_punter_id=p.punter_id)
                                 inner join spst.ca_revenue_interval cac
                                    on (crv.CA_REVENUE_INTERVAL_ID =cac.id)
                                  where p.kontotyp='PERSON'
                                    --and cac.id=1014 -- Omsättning 0 kr
                                    and crv.ca_time_day_id=this_week_last_day_id
                            ),
                cust_channel as (
                           select p.punter_key,
                                  cah.id,
                                  cah.channel_name,
                                  c.channel_key,
                                  pc.partner_channel_key,
                                  ctd.year_week
                                  from spst.customer_channel cch inner join spst.ca_time_day ctd
                                    on (cch.ca_time_day_id=ctd.id)
                                 inner join spst.punter p
                                    on (cch.punter_punter_id=p.punter_id)
                                 inner join spst.ca_channel cah
                                    on (cch.ca_channel_id =cah.id)
                                 left outer join spst.channel c
                                    on (cch.channel_channel_id=c.channel_id)
                                 left outer join partner_channel pc
                                    on (cch.partner_channel_id=pc.partner_channel_id)
                                  where p.kontotyp='PERSON'
                                    and cch.ca_time_day_id=this_week_last_day_id
                            ),
                 cust_together as (
                           select p.punter_key,
                                  cat.id,
                                  cat.together_name,
                                  ctd.year_week
                                  from spst.customer_together ct inner join spst.ca_time_day ctd
                                    on (ct.ca_time_day_id=ctd.id)
                                 inner join spst.punter p
                                    on (ct.punter_punter_id=p.punter_id)
                                 inner join spst.ca_together cat
                                    on (ct.ca_together_id=cat.id)
                                  where p.kontotyp='PERSON'
                                    and ct.ca_time_day_id=this_week_last_day_id
                            ),
                 cust_shared_bet_type as (
                           select csbt.punter_key,
                                  sbt.id,
                                  sbt.display_name as shared_bet_type_name,
                                  ctd.year_week
                             from customer_shared_bet_type csbt 
                                   inner join spst.ca_time_day ctd on (csbt.ca_time_day_id=ctd.id)
                                   inner join shared_bet_type sbt on (sbt.id=csbt.shared_bet_type_id)
                            where ctd.id=this_week_last_day_id
                            )
             select year_week,
                    punter_key,
                    coalesce(max(cluster_id),1008) as cluster_id,  --Inget kluster,
                    coalesce(max(cluster_name),'Inget kluster') as cluster_name,
                    coalesce(max(frequency_id),-1) as frequency_id,
                    coalesce(max(frequency_name),'N/A') as frequency_name,
                    coalesce(max(cycle_id),-1) as cycle_id,
                    coalesce(max(cycle_name),'N/A') as cycle_name,
                    coalesce(max(gallop_id),-1) as gallop_id,
                    coalesce(max(gallop_name),'N/A') as gallop_name,
                    coalesce(max(vr_id),-1) as vr_id,
                    coalesce(max(vr_name),'N/A') as vr_name,
                    coalesce(max(luck_skill_id),-1) as luck_skill_id,
                    coalesce(max(luck_skill_name),'N/A') as luck_skill_name,
                    coalesce(max(revenue_interval_id),1014) as revenue_interval_id,
                    coalesce(max(class),14) as class,
                    coalesce(max(revenue_from),0) as revenue_from,
                    coalesce(max(revenue_to),0) as revenue_to,
                    coalesce(max(channel_type_id),-1) as channel_type_id,
                    coalesce(max(home_channel_key),-1) as home_channel_key,
                    coalesce(max(channel_type_name),'N/A') as channel_type_name,
                    coalesce(max(together_id),-1) as together_id,
                    coalesce(max(together_name),'N/A') as together_name,
                    coalesce(max(home_partner_channel_key),-1) as home_partner_channel_key,
                    coalesce(max(shared_bet_type_id),-1) as shared_bet_type_id,
                    coalesce(max(shared_bet_type_name),'N/A') as shared_bet_type_name
                    from (
             select this_week year_week,
                    coalesce(revi.punter_key, ls.punter_key ,gar.punter_key ,ccc.punter_key ,freq.punter_key,clu.punter_key,cc.punter_key,ct.punter_key,csbt.punter_key) punter_key,
                    clu.cluster_id cluster_id,  --Inget kluster
                    clu.cluster_name cluster_name,
                    freq.frequency_id frequency_id,
                    freq.frequency_name frequency_name,
                    ccc.cycle_id cycle_id,
                    ccc.cycle_name cycle_name,
                    CASE WHEN gar.gallop_and_vr_id=1001 THEN
                          gallop_and_vr_id
                    END gallop_id,
                    CASE WHEN gar.gallop_and_vr_id=1002 THEN
                          gallop_and_vr_id
                    END vr_id,
                    CASE WHEN gar.gallop_and_vr_id=1001 THEN
                          gar.gallop_and_vr_name
                    END gallop_name,
                    CASE WHEN gar.gallop_and_vr_id=1002 THEN
                          gar.gallop_and_vr_name
                    END vr_name,
                    ls.luck_skill_id luck_skill_id,
                    ls.luck_skill_name luck_skill_name,
                    revi.revenue_interval_id revenue_interval_id,
                    revi.class class,
                    revi.revenue_from revenue_from,
                    revi.revenue_to revenue_to,
                    CASE WHEN cc.id=1004 THEN --Hemmabutik
                         null
                    else
                        cc.id
                    end channel_type_id,
                    CASE WHEN cc.id=1004 THEN --Hemmabutik
                         null
                    else
                         cc.channel_name
                    end channel_type_name,
                    CASE WHEN cc.id=1004 THEN --Hemmabutik
                         cc.channel_key
                    else
                         null
                    end home_channel_key,
                    CASE WHEN cc.id=1004 THEN --Hemmabutik
                        cc.partner_channel_key
                    else
                        null
                    end home_partner_channel_key,
                    ct.id together_id,
                    ct.together_name together_name,
                    csbt.id as shared_bet_type_id,
                    csbt.shared_bet_type_name as shared_bet_type_name
              from cust_revenue_interval revi full outer join cust_luck_skill ls on ls.punter_key=revi.punter_key and ls.year_week=revi.year_week
                   full outer join cust_gallop_and_vr gar on ls.punter_key=gar.punter_key and ls.year_week=gar.year_week
                   full outer join cust_customer_cycle ccc on ls.punter_key=ccc.punter_key and ls.year_week=ccc.year_week
                   full outer join cust_freguency freq on ls.punter_key=freq.punter_key and ls.year_week=freq.year_week
                   full outer join cust_cluster clu on ls.punter_key=clu.punter_key and ls.year_week=clu.year_week
                   full outer join cust_channel cc on ls.punter_key=cc.punter_key and ls.year_week=cc.year_week
                   full outer join cust_together ct on ls.punter_key=ct.punter_key and ls.year_week=ct.year_week
                   full outer join cust_shared_bet_type csbt on ls.punter_key=csbt.punter_key and ls.year_week=csbt.year_week
                   )
             group by year_week, punter_key;
             
            l_num_of_rows:=sql%rowcount;        
            
            utilities.reg_batch_log( pin_package => JOB_NAME,
                                     pin_procedure => MODULE_NAME,
                                     pin_started_on => ld_start_time,
                                     pin_step_in_proc => 1,
                                     pin_no_rows => l_num_of_rows,
                                     pio_id => ln_batch_log_id);             

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end load_customer_segment;

------------------------------------------------------------------------------
procedure set_dim_and_meta is

  MODULE_NAME            constant varchar2(30) := 'set_dim_and_meta';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin

  dbms_application_info.set_action(
    action_name => 'set_slowly_changing_dimensions');
  set_slowly_changing_dimensions;

  dbms_application_info.set_action(
    action_name => 'set_meta_data_repository');
  set_meta_data_repository;

exception
  when others then
   dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
   raise;
end set_dim_and_meta;

------------------------------------------------------------------------------
function get_last_analyzed (pin_table_name in varchar2) return date as
ld_last_analyzed date;
begin

  select last_analyzed
    into ld_last_analyzed
    from user_tab_partitions
   where table_name = pin_table_name
     and partition_position = (select max(partition_position) from user_tab_partitions where table_name = pin_table_name);

   return ld_last_analyzed;

end get_last_analyzed;

------------------------------------------------------------------------------
function get_partition_name (pin_table_name in varchar2) return varchar2 as
lv_partition_name varchar2(30);
begin

  select partition_name
    into lv_partition_name
    from user_tab_partitions
   where table_name = pin_table_name
     and partition_position = (select max(partition_position) from user_tab_partitions where table_name = pin_table_name);

   return lv_partition_name;

end get_partition_name;


procedure gather_new_table_stats (pin_table_name in varchar2, pin_partition_name in varchar2) is

lv_partition_name varchar2(30) := pin_partition_name;
begin

   if lv_partition_name is null then
      lv_partition_name := get_partition_name (pin_table_name);
   end if;

   begin
      dbms_stats.gather_table_stats (ownname => 'SPST', tabname => pin_table_name, partname => lv_partition_name, degree => 10);
   exception
      when others then
         null;
   end;

end gather_new_table_stats;


procedure load_partition_base_fact_table(p_date date) is

  MODULE_NAME            constant varchar2(30) := 'load_partition_base_fact_table';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  lv_Partition_Name      VARCHAR2(30);

begin

  dbms_application_info.set_module(module_name => 'customer_activity_job',
                                   action_name => 'insert_one_date_time_day');
  insert_one_date_time_day(p_date);

  commit;

  -- Gather stats from betfact_agg
--  lv_partition_name := get_partition_name ('BETFACT_AGG');
--  gather_new_table_stats ('BETFACT_AGG',lv_partition_name); 
  
  dbms_application_info.set_action(
      action_name => 'load_customer_activity');
  load_customer_activity(p_date);

  commit;

  dbms_application_info.set_action(
      action_name => 'load_customer_activity2');
  load_customer_activity2(p_date);

  commit;

  dbms_application_info.set_action(
      action_name => 'load_cust_together_activity');
  load_cust_together_activity(p_date);

  commit;

  lv_partition_name := get_partition_name ('CA_TIME_DAY');

  gather_new_table_stats ('CUSTOMER_ACTIVITY',          lv_partition_name);
  gather_new_table_stats ('CUSTOMER_ACTIVITY2',         lv_partition_name);
  gather_new_table_stats ('CUSTOMER_TOGETHER_ACTIVITY', lv_partition_name);

exception
  when others then
   dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
   raise;
end load_partition_base_fact_table;
----------------------------------------



--------------------------------------


procedure load_agg_fact_tables(p_date date) is

  MODULE_NAME            constant varchar2(30) := 'load_agg_fact_tables';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  lv_partition_name      varchar2(30);

begin

    lv_partition_name := get_partition_name ('CA_TIME_DAY');

    dbms_application_info.set_action(
      action_name => 'load_customer_revenue');
    load_customer_revenue(p_date);

    commit;

    gather_new_table_stats ('CUSTOMER_REVENUE', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_num_of_game_days');
    load_num_of_game_days(p_date);

    commit;

    gather_new_table_stats ('NUM_OF_GAME_DAYS', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_num_of_game_days_total');
    load_num_of_game_days_total(p_date);

    commit;

    gather_new_table_stats ('NUM_OF_GAME_DAYS_TOTAL', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_nogd_trot_concept');
    load_nogd_trot_concept(p_date);

    commit;

    gather_new_table_stats ('NUM_OF_GAME_DAYS_TROT_CONCEPT', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_nogd_race_type');
    load_nogd_race_type(p_date);

    commit;

    gather_new_table_stats ('NUM_OF_GAME_DAYS_RACE_TYPE', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_nogd_hr_race_type');
    load_nogd_hr_race_type(p_date);

    commit;

    gather_new_table_stats ('NUM_OF_GAME_DAYS_HR_RACE_TYPE', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_nogd_game_type');
    load_nogd_game_type(p_date);

    commit;

    dbms_application_info.set_action(
      action_name => 'load_nogd_channel_rev');
    load_nogd_channel_rev(p_date);

    commit;

    gather_new_table_stats ('NUM_OF_GAME_DAYS_CHANNEL_REV', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_nogd_together');
    load_nogd_together(p_date);

    commit;

    gather_new_table_stats ('NUM_OF_GAME_DAYS_TOGETHER', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_ltgd_together');
    load_ltgd_together(p_date);

    commit;

    gather_new_table_stats ('LATEST_TEAM_GAME_DATE_TOGETHER', lv_partition_name);

    dbms_application_info.set_action(
      action_name => 'load_cust_together_kpi');
    load_cust_together_kpi(p_date);

    commit;

    gather_new_table_stats ('CUSTOMER_TOGETHER_KPI', lv_partition_name);

exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;
end load_agg_fact_tables;

------------------------------------------------------------------------------

procedure load_factless_fact_tables(p_date date) is

  MODULE_NAME            constant varchar2(30) := 'load_factless_fact_tables';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin

    dbms_application_info.set_action(
      action_name => 'load_customer_cluster_1');
    load_customer_cluster_1(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_cluster_2');
    load_customer_cluster_2(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_frequency');
    load_customer_frequency(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_cycle');
    load_customer_cycle(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_rev_interval');
    load_customer_rev_interval(p_date);
    commit;

    /* Replaced by load_customer_bet_mode - keep the table customer_luck_skill for now.
    dbms_application_info.set_action(
      action_name => 'load_customer_luck_skill');
    load_customer_luck_skill(p_date);
    commit;
    */

    dbms_application_info.set_action(
      action_name => 'load_customer_bet_mode');
    load_customer_bet_mode(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_gallop_and_vr');
    load_customer_gallop_and_vr(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_channel');
    load_customer_channel(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_together');
    load_customer_together(p_date);
    commit;
    
    dbms_application_info.set_action(
      action_name => 'load_customer_shared_bet_type');
    load_customer_shared_bet_type(p_date);
    commit;

    dbms_application_info.set_action(
      action_name => 'load_customer_segment');
    load_customer_segment(p_date);
    commit;


exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;
end load_factless_fact_tables;

------------------------------------------------------------------------------

procedure rerun_last_day_of_week(p_date date) is
  l_last_day_of_week_date   date;
  l_last_day_of_week_id     number;

  MODULE_NAME            constant varchar2(30) := 'rerun_last_day_of_week';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin

  l_last_day_of_week_date:=get_last_day_of_week(p_date);
  l_last_day_of_week_id:=get_time_day_id(l_last_day_of_week_date);

  while   is_customer_revenue(l_last_day_of_week_id) or
          is_num_of_game_days(l_last_day_of_week_id) or
          is_num_of_game_days_total(l_last_day_of_week_id)
  loop

    -- rerun last day of week for any week after this date
    if drop_partition_for_date(l_last_day_of_week_date) then
      rebuild_index;
      load_partition_base_fact_table(l_last_day_of_week_date);
      load_agg_fact_tables(l_last_day_of_week_date);
      commit;
      load_factless_fact_tables(l_last_day_of_week_date);
    end if;

    l_last_day_of_week_date := l_last_day_of_week_date + SEVEN;
    l_last_day_of_week_id := get_time_day_id(l_last_day_of_week_date);
    commit;
  end loop;

exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;
end rerun_last_day_of_week;
------------------------------------------------------------------------------
procedure run_last_day_of_week(p_date date) is
  l_last_day_of_week_date   date;
  l_last_day_of_week_id     number;
  l_partition_dropped       boolean := false;

  MODULE_NAME            constant varchar2(30) := 'run_last_day_of_week';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin
  l_last_day_of_week_date := get_last_day_of_week(p_date);
  l_last_day_of_week_id := get_time_day_id(l_last_day_of_week_date);

  -- check if last day of week is in any agg fact table
  if  is_customer_revenue(l_last_day_of_week_id) or
      is_num_of_game_days(l_last_day_of_week_id) or
      is_num_of_game_days_total(l_last_day_of_week_id) or
      is_nogd_trot_concept(l_last_day_of_week_id) then

      rerun_last_day_of_week(p_date);
  else
  -- check to make sure all week is included in base fact table
    if get_number_of_days_in_week(p_date) = SEVEN then
      load_agg_fact_tables(l_last_day_of_week_date);
      commit;
      load_factless_fact_tables(l_last_day_of_week_date);
    end if;
  end if;

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end run_last_day_of_week;

------------------------------------------------------------------------------

procedure init(p_first_date_for_agg_fact date, p_last_date_for_base_fact date) is

  count_rows  pls_integer;
  l_date      date;

  NUM_OF_DAYS_AGO   constant pls_integer := (53*7)+1;
  START_WEEK        constant pls_integer := to_number(to_char(p_first_date_for_agg_fact-NUM_OF_DAYS_AGO,'YYYYIW'));
  END_WEEK          constant pls_integer := to_number(to_char(p_first_date_for_agg_fact,'YYYYIW'));
  START_WEEK_R52    constant pls_integer := to_number(to_char(p_first_date_for_agg_fact-(52*7)+1,'YYYYIW'));
  START_WEEK_R13    constant pls_integer := to_number(to_char(p_first_date_for_agg_fact-(13*7)+1,'YYYYIW'));

  MODULE_NAME            constant varchar2(30) := 'init';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin
  -- last day of week
  if to_number(to_char(p_first_date_for_agg_fact,'d')) = SEVEN then null; else return; end if;

  -- partition and base fact table
  select /*+ first_rows(1) */ count(id) into count_rows
  from ca_time_day where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_activity where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- agg fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_revenue where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days_total where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days_trot_concept where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days_race_type where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_cluster where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- betfact agg
  select /*+ first_rows(1) */ count(ba_time_key) into count_rows
  from betfact_agg where ba_time_key =
    to_char(p_first_date_for_agg_fact - SEVEN * ROLLING_LONG_WEEK,YYYYMMDD) and rownum < TWO;
  if count_rows = ONE then null; else return; end if;

  set_dim_and_meta;

  l_date:=p_first_date_for_agg_fact-NUM_OF_DAYS_AGO;

  while l_date<=p_first_date_for_agg_fact loop
    load_partition_base_fact_table(l_date);
    l_date:=l_date+1;
    commit;
  end loop;


  dbms_application_info.set_module(module_name => 'customer_activity_job',
                                   action_name => 'init_customer_revenue');
  init_customer_revenue(START_WEEK, END_WEEK, START_WEEK_R52, START_WEEK_R13, p_first_date_for_agg_fact);
  commit;

  dbms_application_info.set_action(
      action_name => 'init_nogd');
  init_nogd(START_WEEK, END_WEEK, START_WEEK_R52, START_WEEK_R13, p_first_date_for_agg_fact);
  commit;

  dbms_application_info.set_action(
      action_name => 'init_nogd_total');
  init_nogd_total(START_WEEK, END_WEEK, START_WEEK_R52, START_WEEK_R13, p_first_date_for_agg_fact);
  commit;

  dbms_application_info.set_action(
      action_name => 'init_nogd_trot_concept');
  init_nogd_trot_concept(START_WEEK, END_WEEK, START_WEEK_R52, START_WEEK_R13, p_first_date_for_agg_fact);
  commit;

  dbms_application_info.set_action(
      action_name => 'init_nogd_race_type');
  init_nogd_race_type(START_WEEK, END_WEEK, START_WEEK_R52, START_WEEK_R13, p_first_date_for_agg_fact);

  commit;

  while l_date <= p_last_date_for_base_fact loop

    dbms_application_info.set_action(
      action_name => 'load_partition_base_fact_table');
    load_partition_base_fact_table(l_date);

    dbms_application_info.set_action(
      action_name => 'load_customer_revenue');
    load_customer_revenue(l_date);

    dbms_application_info.set_action(
      action_name => 'load_num_of_game_days');
    load_num_of_game_days(l_date);

    dbms_application_info.set_action(
      action_name => 'load_num_of_game_days_total');
    load_num_of_game_days_total(l_date);

    dbms_application_info.set_action(
      action_name => 'load_nogd_trot_concept');
    load_nogd_trot_concept(l_date);

    dbms_application_info.set_action(
      action_name => 'load_nogd_race_type');
    load_nogd_race_type(l_date);

    l_date:=l_date+1;
    commit;
  end loop;

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end init;

------------------------------------------------------------------------------

procedure deployment_initialization_2 is

  count_rows          pls_integer;
  min_ca_time_day_id  number;
  l_date              date;

  MODULE_NAME            constant varchar2(30) := 'deployment_initialization';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin

  -- factless fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_frequency where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_cycle where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_luck_skill where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  dbms_application_info.set_module(module_name => 'customer_activity_job',
                                   action_name => 'deployment_initialization');
  set_dim_and_meta;

  select min(ca_time_day_id) into min_ca_time_day_id from customer_cluster;
  select full_date into l_date from ca_time_day where id = min_ca_time_day_id;

  while l_date < trunc(sysdate) loop

    dbms_application_info.set_action(
      action_name => 'load_customer_frequency');
    load_customer_frequency(l_date);

    dbms_application_info.set_action(
      action_name => 'load_customer_cycle');
    load_customer_cycle(l_date);

    dbms_application_info.set_action(
      action_name => 'load_customer_luck_skill');
    load_customer_luck_skill(l_date);

    dbms_application_info.set_action(
      action_name => 'load_customer_rev_interval');
    load_customer_rev_interval(l_date);

    commit;

    l_date:=l_date+1;
  end loop;

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end deployment_initialization_2;

------------------------------------------------------------------------------

procedure deployment_initialization_3 is

  count_rows          pls_integer;
  min_ca_time_day_id  number;
  max_ca_time_day_id  number;
  l_date              date;
  max_date            date;

  NUM_OF_DAYS_AGO   constant pls_integer := (53*7)+1;

  start_week        pls_integer;
  end_week          pls_integer;
  start_week_r52    pls_integer;
  start_week_r13    pls_integer;

  MODULE_NAME            constant varchar2(30) := 'deployment_initialization';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin

  -- agg fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days_hr_race_type where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days_game_type where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- factless fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_gallop_and_vr where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select min(ca_time_day_id) into min_ca_time_day_id from num_of_game_days;
  select full_date into l_date from ca_time_day where id = min_ca_time_day_id;

  start_week        := to_number(to_char(l_date-NUM_OF_DAYS_AGO,'YYYYIW'));
  end_week          := to_number(to_char(l_date,'YYYYIW'));
  start_week_r52    := to_number(to_char(l_date-(52*7)+1,'YYYYIW'));
  start_week_r13    := to_number(to_char(l_date-(13*7)+1,'YYYYIW'));

  dbms_application_info.set_module(module_name => 'customer_activity_job',
                                   action_name => 'deployment_initialization');

  dbms_application_info.set_action(
      action_name => 'init_nogd_hr_race_type');
  init_nogd_hr_race_type(start_week, end_week, start_week_r52, start_week_r13, l_date);
  commit;

  dbms_application_info.set_action(
      action_name => 'init_nogd_game_type');
  init_nogd_game_type(start_week, end_week, start_week_r52, start_week_r13, l_date);
  commit;

  select max(ca_time_day_id) into max_ca_time_day_id from num_of_game_days;
  select full_date into max_date from ca_time_day where id = max_ca_time_day_id;

  l_date:=l_date+1;

  while l_date <= max_date loop

    dbms_application_info.set_action(
      action_name => 'load_nogd_hr_race_type');
    load_nogd_hr_race_type(l_date);

    dbms_application_info.set_action(
      action_name => 'load_nogd_game_type');
    load_nogd_game_type(l_date);

    commit;

    l_date:=l_date+1;
  end loop;

  set_dim_and_meta;

  select min(ca_time_day_id) into min_ca_time_day_id from customer_cluster;
  select full_date into l_date from ca_time_day where id = min_ca_time_day_id;

  select max(ca_time_day_id) into max_ca_time_day_id from customer_cluster;
  select full_date into max_date from ca_time_day where id = max_ca_time_day_id;

  while l_date <= max_date loop

    dbms_application_info.set_action(
      action_name => 'load_customer_gallop_and_vr');
    load_customer_gallop_and_vr(l_date);

    commit;

    l_date:=l_date+1;
  end loop;

exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end deployment_initialization_3;

------------------------------------------------------------------------------

procedure deployment_init_together is

  l_mindate date;
  l_mindate_td date;
  max_date date;
  max_ca_time_day_id number;
  count_rows pls_integer;

begin

  -- base fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_together_activity where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- agg fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days_together where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from latest_team_game_date_together where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- factless fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_together where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- kpi tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_together_kpi where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;


  select min(speldatum) into l_mindate from MDB.speltransaktioner where tekniska_kanaler_id in (19,20);

  select min(full_date) into l_mindate_td from ca_time_day;

  if l_mindate_td>l_mindate then l_mindate:=l_mindate_td; end if;

  select max(ca_time_day_id) into max_ca_time_day_id from customer_activity;

  select full_date into max_date from ca_time_day where id = max_ca_time_day_id;


-- Basefact and agg fact
  while l_mindate<=max_date loop
    load_cust_together_activity(l_mindate);
    load_nogd_together(l_mindate);
    load_ltgd_together(l_mindate);
    load_cust_together_kpi(l_mindate);
    l_mindate:=l_mindate+1;
    commit;
  end loop;

end deployment_init_together;

------------------------------------------------------------------------------
procedure deployment_initialization_4 is

  count_rows          pls_integer;
  min_ca_time_day_id  number;
  max_ca_time_day_id  number;
  l_date              date;
  max_date            date;

  NUM_OF_DAYS_AGO   constant pls_integer := (53*7)+1;

  start_week        pls_integer;
  end_week          pls_integer;
  start_week_r52    pls_integer;
  start_week_r13    pls_integer;

  MODULE_NAME            constant varchar2(30) := 'deployment_initialization';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin

  deployment_init_together;

  -- base fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_activity2 where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- agg fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from num_of_game_days_channel_rev where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  -- factless fact tables
  select /*+ first_rows(1) */ count(ca_time_day_id) into count_rows
  from customer_channel where rownum < TWO;
  if count_rows = ZERO then null; else return; end if;

  select min(ca_time_day_id) into min_ca_time_day_id from customer_activity;
  select full_date into l_date from ca_time_day where id = min_ca_time_day_id;

  select max(ca_time_day_id) into max_ca_time_day_id from customer_activity;
  select full_date into max_date from ca_time_day where id = max_ca_time_day_id;

  dbms_application_info.set_module(module_name => 'customer_activity_job',
                                   action_name => 'deployment_initialization');

  while l_date <= max_date loop
    load_customer_activity2(l_date);
    l_date:=l_date+1;
    commit;
  end loop;

  select min(ca_time_day_id) into min_ca_time_day_id from num_of_game_days;
  select full_date into l_date from ca_time_day where id = min_ca_time_day_id;

  start_week        := to_number(to_char(l_date-NUM_OF_DAYS_AGO,'YYYYIW'));
  end_week          := to_number(to_char(l_date,'YYYYIW'));
  start_week_r52    := to_number(to_char(l_date-(52*7)+1,'YYYYIW'));
  start_week_r13    := to_number(to_char(l_date-(13*7)+1,'YYYYIW'));

  dbms_application_info.set_action(
      action_name => 'init_nogd_channel_rev');
  init_nogd_channel_rev(start_week, end_week, start_week_r52, start_week_r13, l_date);
  commit;

  select max(ca_time_day_id) into max_ca_time_day_id from num_of_game_days;
  select full_date into max_date from ca_time_day where id = max_ca_time_day_id;

  l_date:=l_date+1;

  while l_date <= max_date loop

    dbms_application_info.set_action(
      action_name => 'load_nogd_channel_rev');
    load_nogd_channel_rev(l_date);

    commit;

    l_date:=l_date+1;
  end loop;

  set_dim_and_meta;

  select min(ca_time_day_id) into min_ca_time_day_id from customer_cluster;
  select full_date into l_date from ca_time_day where id = min_ca_time_day_id;

  select max(ca_time_day_id) into max_ca_time_day_id from customer_cluster;
  select full_date into max_date from ca_time_day where id = max_ca_time_day_id;

  while l_date <= max_date loop

    dbms_application_info.set_action(
      action_name => 'load_customer_channel');
    load_customer_channel(l_date);
    load_customer_together(l_date);

    commit;

    l_date:=l_date+1;
  end loop;


exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end deployment_initialization_4;


------------------------------------------------------------------------------
procedure deployment_initialization_5 is

  count_rows          pls_integer;
  min_ca_time_day_id  number;
  l_date              date;

  l_mindate date;
  l_mindate_td date;
  max_date date;
  max_ca_time_day_id number;
  count_rows pls_integer;

  MODULE_NAME            constant varchar2(30) := 'deployment_initialization';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin

  execute immediate 'truncate table spst.customer_together_kpi';

  select min(speldatum) into l_mindate from MDB.speltransaktioner where tekniska_kanaler_id in (19,20);

  select min(full_date) into l_mindate_td from ca_time_day;

  if l_mindate_td>l_mindate then l_mindate:=l_mindate_td; end if;

  select max(ca_time_day_id) into max_ca_time_day_id from customer_activity;

  select full_date into max_date from ca_time_day where id = max_ca_time_day_id;

  while l_mindate<=max_date loop
    load_cust_together_kpi(l_mindate);
    l_mindate:=l_mindate+1;
    commit;
  end loop;

--------------------------------------------------------------------------------

  execute immediate 'truncate table spst.customer_frequency';

  select min(ca_time_day_id) into min_ca_time_day_id from customer_cluster;
  select full_date into l_date from ca_time_day where id = min_ca_time_day_id;

  while l_date < trunc(sysdate) loop

    dbms_application_info.set_action(
      action_name => 'load_customer_frequency');
    load_customer_frequency(l_date);

    commit;

    l_date:=l_date+1;
  end loop;


exception
  when others then
       dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
       raise;
end deployment_initialization_5;

procedure init_punter_first_bet (p_date_to in date) is

  MODULE_NAME            constant varchar2(30) := 'init_punter_first_bet';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  l_date_to_char         varchar2(8) := to_char(p_date_to,'yyyymmdd');
  l_tme_time_id_to       varchar2(10) := to_char(p_date_to+1,'yyyymmdd') || '00';

begin

  insert into spst.punter_first_bet_horse (punter_key, punter_id, first_bet_date, tsn, master_tsn, found_in_mdb, channel_type, bet_mode)
  select punter_key, 
         punter_id, 
         first_bet_time, 
         tsn, 
         master_tsn, 
         found_in_mdb,
         channel_type,
         bet_mode
    from (select punter_key, 
                 punter_id, 
                 first_bet_time, 
                 tsn, 
                 master_tsn, 
                 found_in_mdb, 
                 channel_type,
                 bet_mode,
                 row_number () over (partition by punter_key order by first_bet_time) rwn
            from ((select punter_key, 
                          punter_id, 
                          first_bet_time, 
                          tsn, 
                          master_tsn, 
                          found_in_mdb,
                          channel_type,
                          bet_mode
                     from (select punter_key,
                                  punter_id,
                                  first_bet_time,
                                  tsn,
                                  master_tsn,
                                  found_in_mdb,
                                  channel_type,
                                  bet_mode,
                                  row_number () over (partition by punter_key order by first_bet_time) rwn
                             from (select b.punter_key punter_key, 
                                          a.pun_punter_id punter_id, 
                                          to_date(to_char(to_date(substr(tme_time_id,1,8),'yyyy-mm-dd') + 1/24*to_number(substr(tme_time_id,9,2)),'yyyymmddhh24') || nvl(bet_minute,'00') || '00','yyyy-mm-dd hh24:mi:ss')  first_bet_time,
                                          a.tsn_key tsn,
                                          a.master_tsn_key master_tsn,
                                          'N' found_in_mdb,
                                          d.can_type channel_type,
                                          e.display_name as bet_mode
                                      from spst.betfact a, spst.punter b, spst.program c, spst.channel d, spst.bet_mode e
                                     where a.pun_punter_id = b.punter_id
                                       and b.kontotyp = 'PERSON'
                                       and a.pun_punter_id > 0
                                       and c.prg_date between '20150101' and '20181231' --l_date_char 
                                       and a.prg_program_id = c.program_id
                                       and a.can_channel_id=d.channel_id
                                       and a.bet_mode_id=e.id
                                       and a.tme_time_id between '2014122200' and '2019010100')) x
                            where x.rwn = 1)
            union all
          (select punter_key, 
                  punter_id, 
                  first_bet_time, 
                  tsn, 
                  master_tsn, 
                  found_in_mdb,
                  channel_type,
                  bet_mode
            from (select punter_key, 
                         punter_id, 
                         tsn_key tsn, 
                         master_tsn_key master_tsn,
                         bet_time_mdb first_bet_time, 
                         found_in_mdb,
                         channel_type,
                         bet_mode,
                         row_number () over (partition by punter_key order by bet_time_mdb) rwn
                    from (select punter_key, 
                                 punter_id, 
                                 tsn_key, 
                                 master_tsn_key,
                                 case 
                                   when mdb_speltrans.transts is not null then 'J'
                                   else 'N' 
                                 end found_in_mdb,
                                 channel_type,
                                 bet_mode,
                                 coalesce(mdb_speltrans.transts,bet_time_betfact) bet_time_mdb
                            from (select distinct b.acc_index, 
                                                  b.punter_key, 
                                                  b.punter_id, 
                                                  a.tsn_key,
                                                  a.master_tsn_key,
                                                  case
                                                    when tc_id in (14,16,96,70,21,22) then a.master_tsn_key
                                                    when (tc_id is null or tc_id=6) and not exists (select 1
                                                                                                      from spst.program_pool pp
                                                                                                     where pp.prm_program_id=c.program_id
                                                                                                       and offering_id is not null) then ((select new_tsn_grp_1 || new_tsn_grp_2 || new_tsn_grp_3 || new_tsn_grp_4
                                                                                                                                             from (select substr(tsn_grp1,4,1)||substr(tsn_grp1,3,1)||substr(tsn_grp1,2,1)||substr(tsn_grp1,1,1) new_tsn_grp_1,
                                                                                                                                                          substr(tsn_grp4,2,1)||substr(tsn_grp4,3,1)||substr(tsn_grp4,4,1)||substr(tsn_grp4,1,1) new_tsn_grp_2,
                                                                                                                                                          substr(tsn_grp3,3,1)||substr(tsn_grp3,4,1)||substr(tsn_grp3,1,1)||substr(tsn_grp3,2,1) new_tsn_grp_3,
                                                                                                                                                          substr(tsn_grp2,2,1)||substr(tsn_grp2,1,1)||substr(tsn_grp2,4,1)||substr(tsn_grp2,3,1) new_tsn_grp_4
                                                                                                                                                     from (select substr(tsn,1,4) tsn_grp1,
                                                                                                                                                                  substr(tsn,5,4) tsn_grp2, 
                                                                                                                                                                  substr(tsn,9,4) tsn_grp3,
                                                                                                                                                                  substr(tsn,13,4) tsn_grp4
                                                                                                                                                             from (select a.tsn_key as tsn 
                                                                                                                                                                     from dual)))))
                                                    else a.tsn_key
                                                  end tsn_key_to_compare,
                                                  c.prg_date prg_date, 
                                                  to_date(to_char(to_date(substr(tme_time_id,1,8),'yyyy-mm-dd') + 1/24*to_number(substr(tme_time_id,9,2)),'yyyymmddhh24') || nvl(bet_minute,'00') || '00','yyyy-mm-dd hh24:mi:ss')  bet_time_betfact,
                                                  d.can_type channel_type,
                                                  e.display_name as bet_mode
                                    from spst.betfact a, spst.punter b, spst.program c, spst.channel d, bet_mode e
                                   where a.pun_punter_id = b.punter_id
                                     and b.kontotyp = 'PERSON'
                                     and a.pun_punter_id > 0
                                     and c.prg_date between '20190101' and l_date_to_char 
                                     and a.prg_program_id = c.program_id
                                     and a.can_channel_id=d.channel_id
                                     and a.bet_mode_id=e.id
                                     --and b.punter_key not in (select punter_key from punter_first_bet_horse)
                                     and a.tme_time_id between '2018122200' and l_tme_time_id_to) stat_bets 
                               left join (select transts, 
                                                 konto_kontoindex, 
                                                 case
                                                   when s.tekniska_kanaler_id in (19, 20, 24) then substr (s.tsn, 1,instr (s.tsn, 'M') - 1)
                                                   else s.tsn
                                                 end tsn
                                            from mdb.speltransaktioner s
                                           where s.program_id  in (select id from mdb.program where datum between to_date('2019-01-01','yyyy-mm-dd') and p_date_to or id in (-1,-2)) 
                                             and s.speldatum between to_date('2018-12-22','yyyy-mm-dd') and p_date_to+1 
                                             and s.konto_kontoindex is not null) mdb_speltrans
                              on stat_bets.acc_index = mdb_speltrans.konto_kontoindex
                             and stat_bets.tsn_key_to_compare = mdb_speltrans.tsn))
                   where rwn = 1) ) )
   where rwn = 1;

exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;
end init_punter_first_bet;

------------------------------------------------------------------------------

procedure load_punter_first_bet (p_date in date) is

MODULE_NAME            constant varchar2(30) := 'load_punter_first_bet';
JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
l_date_char            varchar2(8) := to_char(p_date,'yyyymmdd');
l_tme_time_id_from     varchar2(10) := to_char(p_date-7,'yyyymmdd') || '00';
l_tme_time_id_to       varchar2(10) := to_char(p_date+1,'yyyymmdd') || '00';

ln_row_count           number;
begin

   insert into gtt_first_bet_betfact (acc_index, punter_key, punter_id, tsn_key, master_tsn_key, tsn_key_to_compare, prg_date,bet_time_betfact, channel_type, bet_mode)
     (select distinct b.acc_index, 
                      b.punter_key, 
                      b.punter_id, 
                      a.tsn_key,
                      a.master_tsn_key,
                      case
                        when tc_id in (14,16,96,70,21,22) then a.master_tsn_key
                        when (tc_id is null or tc_id=6) and not exists (select 1
                                                                          from spst.program_pool pp
                                                                         where pp.prm_program_id=c.program_id
                                                                           and offering_id is not null) then ((select new_tsn_grp_1 || new_tsn_grp_2 || new_tsn_grp_3 || new_tsn_grp_4
                                                                                                                 from (select substr(tsn_grp1,4,1)||substr(tsn_grp1,3,1)||substr(tsn_grp1,2,1)||substr(tsn_grp1,1,1) new_tsn_grp_1,
                                                                                                                              substr(tsn_grp4,2,1)||substr(tsn_grp4,3,1)||substr(tsn_grp4,4,1)||substr(tsn_grp4,1,1) new_tsn_grp_2,
                                                                                                                              substr(tsn_grp3,3,1)||substr(tsn_grp3,4,1)||substr(tsn_grp3,1,1)||substr(tsn_grp3,2,1) new_tsn_grp_3,
                                                                                                                              substr(tsn_grp2,2,1)||substr(tsn_grp2,1,1)||substr(tsn_grp2,4,1)||substr(tsn_grp2,3,1) new_tsn_grp_4
                                                                                                                         from (select substr(tsn,1,4) tsn_grp1,
                                                                                                                                      substr(tsn,5,4) tsn_grp2, 
                                                                                                                                      substr(tsn,9,4) tsn_grp3,
                                                                                                                                      substr(tsn,13,4) tsn_grp4
                                                                                                                                 from (select a.tsn_key as tsn 
                                                                                                                                         from dual)))))
                      else a.tsn_key
                      end tsn_key_to_compare,
                      c.prg_date prg_date, 
                      to_date(to_char(to_date(substr(tme_time_id,1,8),'yyyy-mm-dd') + 1/24*to_number(substr(tme_time_id,9,2)),'yyyymmddhh24') || nvl(bet_minute,'00') || '00','yyyy-mm-dd hh24:mi:ss')  bet_time_betfact,
                      case 
                        when d.can_type = 'E-kanaler' then
                          case when a.actual_sales_channel_id = 2 then 'Ombud'
                          else d.can_type
                          end
                        else d.can_type
                      end can_type,
                      e.display_name as bet_mode
        from spst.betfact a, spst.punter b, spst.program c, spst.channel d, spst.bet_mode e
       where a.pun_punter_id = b.punter_id
         and b.kontotyp = 'PERSON'
         and a.pun_punter_id > 0
         and c.prg_date = l_date_char
         and a.prg_program_id = c.program_id
         and a.can_channel_id=d.channel_id
         and a.bet_mode_id=e.id
         and a.tme_time_id between l_tme_time_id_from and l_tme_time_id_to);
         
   insert into gtt_first_bet_speltransaktioner (transts, konto_kontoindex, tsn)
     (select transts, 
             konto_kontoindex, 
             case
               when s.tekniska_kanaler_id in (19, 20, 24, 25, 27) then substr (s.tsn, 1,instr (s.tsn, 'M') - 1)
             else s.tsn
             end tsn
        from mdb.speltransaktioner s
       where s.program_id  in (select id 
                                 from mdb.program 
                                where datum = p_date or id in (-1,-2))
         and s.speldatum between p_date-7 and p_date+1
         and s.konto_kontoindex is not null);
         
   merge into spst.punter_first_bet_horse pfbh
   using (select punter_key, punter_id, bet_time_mdb, tsn, master_tsn, found_in_mdb, channel_type, bet_mode
            from  (select punter_key, 
                          punter_id, 
                          tsn_key tsn, 
                          master_tsn_key master_tsn,
                          bet_time_mdb, 
                          found_in_mdb,
                          channel_type,
                          bet_mode,
                          row_number () over (partition by punter_key order by bet_time_mdb) rwn
                     from (select bets.punter_key, 
                                  bets.punter_id, 
                                  bets.tsn_key, 
                                  bets.master_tsn_key,
                                  case 
                                    when bets.transts is not null then 'J'
                                  else 'N' 
                                  end found_in_mdb,
                                  coalesce(bets.transts,bets.bet_time_betfact) bet_time_mdb,
                                  bets.channel_type,
                                  bets.bet_mode
                             from (select stat_bets.punter_key, 
                                          stat_bets.punter_id,
                                          stat_bets.tsn_key,
                                          stat_bets.master_tsn_key,
                                          speltrans.transts,
                                          stat_bets.bet_time_betfact,
                                          stat_bets.channel_type,
                                          stat_bets.bet_mode
                                     from gtt_first_bet_betfact stat_bets 
                                          left join gtt_first_bet_speltransaktioner speltrans
                                                 on stat_bets.acc_index = speltrans.konto_kontoindex
                                                and stat_bets.tsn_key_to_compare = speltrans.tsn) bets)) 
                    where rwn = 1) first_bet
   on (pfbh.punter_key = first_bet.punter_key)
     when not matched then
       insert (pfbh.punter_key,
               pfbh.punter_id,
               pfbh.first_bet_date,
               pfbh.tsn,
               pfbh.master_tsn,
               pfbh.found_in_mdb,
               pfbh.channel_type,
               pfbh.bet_mode)
       values (first_bet.punter_key,
               first_bet.punter_id,
               first_bet.bet_time_mdb,
               first_bet.tsn,
               first_bet.master_tsn,
               first_bet.found_in_mdb,
               first_bet.channel_type,
               first_bet.bet_mode)
     when matched then
       update set pfbh.first_bet_date = first_bet.bet_time_mdb,
                  pfbh.tsn = first_bet.tsn,
                  pfbh.master_tsn = first_bet.master_tsn,
                  pfbh.punter_id = first_bet.punter_id,
                  pfbh.found_in_mdb = first_bet.found_in_mdb,
                  pfbh.channel_type = first_bet.channel_type,
                  pfbh.bet_mode = first_bet.bet_mode
            where first_bet.bet_time_mdb < pfbh.first_bet_date; 
   
   ln_row_count := sql%rowcount;
   spst.customer_journey_job.log_customer_journey_load(trunc(p_date), ln_row_count, SPST.CUSTOMER_JOURNEY_JOB.FIRST_BET, SPST.CUSTOMER_JOURNEY_JOB.VERTICAL_ID_HORSE);
                  
exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;

end load_punter_first_bet;


------------------------------------------------------------------------------

procedure load_ca_daily_bets (p_date in date) is

  MODULE_NAME             constant varchar2(30) := 'load_ca_daily_bets';
  JOB_MODULE              constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time           date := sysdate;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
   
begin
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Load date: '||to_char(p_date,'yyyy-mm-dd'));
                             
        insert into ca_daily_bets ( ca_time_day_id,
                                    punter_key,
                                    punter_punter_id,
                                    num_of_plays,
                                    bet_amount ) 
        select customer_activity_job.get_time_day_id(prg.datum) as ca_time_day_id,
               p.punter_key,
               max(p.punter_id) as punter_id,
               count(distinct coalesce(to_char(gruppspelid), tsn||to_char(konto_kontoindex))) num_of_plays,
               sum(belopp) bet_amount
          from mdb.speltransaktioner st, punter_current p, mdb.program prg
         where p.acc_index=st.konto_kontoindex
           and konto_kontoindex is not null 
           and st.program_id=prg.id
           and prg.datum=p_date
        group by prg.datum, punter_key           
        union all
        select customer_activity_job.get_time_day_id(speldatum) as ca_time_day_id,
               p.punter_key,
               max(p.punter_id) as punter_id,
               count(distinct coalesce(to_char(gruppspelid), tsn||to_char(konto_kontoindex))) num_of_plays,
               sum(belopp) bet_amount
          from mdb.speltransaktioner st, punter_current p, mdb.program prg
         where p.acc_index=st.konto_kontoindex
           and konto_kontoindex is not null 
           and st.program_id=prg.id
           and st.speldatum=p_date
           and st.program_id in (-1,-2)  --VR        
        group by speldatum, punter_key; 
        
        l_num_of_rows := sql%rowcount;
        
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => l_num_of_rows,
                                 pio_id => ln_batch_log_id);        

exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;

end load_ca_daily_bets;

------------------------------------------------------------------------------

procedure load_ca_daily_bets_betform (p_date in date) is

  MODULE_NAME            constant varchar2(30) := 'load_ca_daily_bets_betform';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
  l_date_char            varchar2(8) := to_char(p_date,'yyyymmdd');
  l_tme_time_id_from     varchar2(10) := to_char(p_date-7,'yyyymmdd') || '00';
  l_tme_time_id_to       varchar2(10) := to_char(p_date+1,'yyyymmdd') || '00';
   
begin

    ld_start_time:=sysdate;
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id);
                             
    insert into ca_daily_bets_betform (ca_time_day_id,
                                       punter_punter_id,
                                       punter_key,
                                       betform_id,
                                       deduction,
                                       bet_amount,
                                       net_amount)
    select customer_activity_job.get_time_day_id(p_date) as ca_time_day_id,
           punter_id,
           punter_key,
           betform_id,
           deduction,
           bet_amount,
           net_amount
      from (select b.prg_date prg_date, 
                   c.punter_key punter_key, 
                   b.betform_id betform_id, 
                   c.punter_id punter_id, 
                   b.deduction/100 deduction, 
                   sum(bet_amount) bet_amount, 
                   sum(bet_amount)*(b.deduction/100) net_amount
              from betfact a, program_deduction_view b, punter c
             where a.tme_time_id between l_tme_time_id_from and l_tme_time_id_to
               and b.prg_date=l_date_char
               and a.prg_program_id=b.program_id
               and a.bef_betform_id=b.betform_id
               and a.pun_punter_id=c.punter_id
               and c.kontotyp = 'PERSON'
               and a.pun_punter_id>0
             group by b.prg_date, c.punter_key, c.punter_id, b.betform_id, b.deduction/100);
        
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => l_num_of_rows,
                                 pio_id => ln_batch_log_id);        

exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;

end load_ca_daily_bets_betform;

------------------------------------------------------------------------------

procedure init_load_ca_daily_bets (p_date_to in date) is

  MODULE_NAME            constant varchar2(30) := 'init_load_ca_daily_bets';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;
  ld_start_time date;
  ln_batch_log_id         batch_log.id%type;
  l_num_of_rows           number;
   
begin

    ld_start_time:=sysdate;
    
    utilities.reg_batch_log( pin_package => JOB_NAME,
                             pin_procedure => MODULE_NAME,
                             pin_started_on => ld_start_time,
                             pin_step_in_proc => 1,
                             pin_no_rows => null,
                             pio_id => ln_batch_log_id,
                             pin_info         => 'Init end date: '||to_char(p_date_to,'yyyy-mm-dd'));
                             
                             
        insert into ca_daily_bets ( ca_time_day_id,
                                    punter_key,
                                    punter_punter_id,
                                    num_of_plays,
                                    bet_amount ) 
        select customer_activity_job.get_time_day_id(prg.datum) as ca_time_day_id,
               p.punter_key,
               max(p.punter_id) as punter_id,
               count(distinct coalesce(to_char(gruppspelid), tsn||to_char(konto_kontoindex))) num_of_plays,
               sum(belopp) bet_amount
          from mdb.speltransaktioner st, punter_current p, mdb.program prg
         where p.acc_index=st.konto_kontoindex
           and konto_kontoindex is not null 
           and st.program_id=prg.id
           and prg.datum between to_date('20180101','yyyymmdd') and p_date_to
        group by prg.datum, punter_key           
        union all
        select customer_activity_job.get_time_day_id(speldatum) as ca_time_day_id,
               p.punter_key,
               max(p.punter_id) as punter_id,
               count(distinct coalesce(to_char(gruppspelid), tsn||to_char(konto_kontoindex))) num_of_plays,
               sum(belopp) bet_amount
          from mdb.speltransaktioner st, punter_current p, mdb.program prg
         where p.acc_index=st.konto_kontoindex
           and konto_kontoindex is not null 
           and st.program_id=prg.id
           and st.speldatum between to_date('20180101','yyyymmdd') and p_date_to
           and st.program_id in (-1,-2)  --VR        
        group by speldatum, punter_key; 
         
        l_num_of_rows := sql%rowcount;
        
        utilities.reg_batch_log( pin_package => JOB_NAME,
                                 pin_procedure => MODULE_NAME,
                                 pin_started_on => ld_start_time,
                                 pin_step_in_proc => 1,
                                 pin_no_rows => l_num_of_rows,
                                 pio_id => ln_batch_log_id);        

exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
    raise;

end init_load_ca_daily_bets;

procedure do(p_date date) is

  is_partition_dropped      boolean;
  MODULE_NAME            constant varchar2(30) := 'do';
  JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin
  set_dim_and_meta;
  is_partition_dropped := drop_partition_for_date(p_date);

 -- execute immediate 'alter session enable parallel query';
 -- execute immediate 'alter session enable parallel dml';
 execute immediate 'alter session set "_optimizer_gather_stats_on_conventional_dml" = FALSE';
 execute immediate 'alter session set "_optimizer_use_stats_on_conventional_dml" = FALSE';

   
  load_partition_base_fact_table(p_date);
  COMMIT;

  if is_partition_dropped then
    run_last_day_of_week(p_date);
  else
    load_agg_fact_tables(p_date);
    commit;
    load_factless_fact_tables(p_date);
  end if;
  
  -- Publish customer shared bet type changes.  
  dmspst.customer_journey_publish_pkg.customer_shared_bet_type(p_date, 'DELTA');
  commit;
  -- Publish customer horse bet mode changes.
  dmspst.customer_journey_publish_pkg.customer_horse_bet_mode(p_date, 'DELTA');
  commit;
  
  load_ca_daily_bets(p_date);
  load_ca_daily_bets_betform(p_date);
  load_punter_first_bet(p_date);
  commit;
  customer_journey_job.load_customer_journey_horse(p_date);

  commit;

exception
  when others then
    dmspst.Crm_Laddning.FELHANTERING(SQLCODE, SQLERRM, JOB_NAME, MODULE_NAME, 'DB',' meddelande');
	raise;

end do;
------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
procedure main_job(p_procid number, p_debug boolean default false) is

exc_no_start           exception;
l_batchresid           number;
l_procid               number;
l_date                 date;
MODULE_NAME            constant varchar2(30) := 'Main_job';
JOB_MODULE             constant varchar2(100) := 'SPST.'||JOB_NAME||'.'||MODULE_NAME;

begin
 dmspst.stat_felhantering.initsessionvar(JOB_MODULE,p_procid);
 -- initiera batchen...
 if not p_debug then
   if not dmspst.stat_batchprocess.startabatch(p_procid,true) then
     raise exc_no_start;
   end if;
   dmspst.stat_batchprocess.uppdaterabatchres(l_batchresid, p_procid,null,null,null,sysdate);
 end if;
  -----------------------------------
  -- S T A R T
  -----------------------------------
  select ladd_datum
    into l_date
    from dmspst.laddsystem
   where proc_id=p_procid;

  do(l_date);

 if not p_debug then
   dmspst.stat_batchprocess.uppdaterabatchres(l_batchresid,null,null,'Klar '||JOB_NAME,null,null,sysdate);
   dmspst.stat_batchprocess.avslutabatch(p_procid);
   commit;
 end if;
 -- Proceduren är klar och loggning sker i laddsystem och laddsystem_log!
 -- fil/system lyssnaren startar i sin tur nya processer enligt tabellverket.
 if not p_debug then
    dmspst.crm_laddning.loggning(p_procid);
 end if;
 dmspst.stat_felhantering.avslutasessionvar;
exception
  when exc_no_start then
    null;
  when others then
  dmspst.crm_laddning.felhantering(p_procid,
		  			  			   sqlcode,
		  			   			   sqlerrm,
					   			   JOB_NAME,
					   			   MODULE_NAME);

end main_job;

end customer_activity_job;
