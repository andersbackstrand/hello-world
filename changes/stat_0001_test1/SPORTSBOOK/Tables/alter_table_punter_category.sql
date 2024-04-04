declare
ln_count number;
lv_table_name varchar2(255) := 'PUNTER_CATEGORY';
lv_column_name varchar2(255) := 'DISPLAY_NAME';
begin

   select count(*) 
     into ln_count
     from user_tab_columns 
    where table_name = lv_table_name
     and  column_name=lv_column_name;

   if ln_count = 0 then
      execute immediate 'alter table '||lv_table_name||' add ('||lv_column_name||' varchar2(50 char))';
   end if; 

end;
