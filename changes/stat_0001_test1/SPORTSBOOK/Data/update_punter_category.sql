declare
begin
    UPDATE PUNTER_CATEGORY SET DISPLAY_NAME='Normal' WHERE ID=1001;
    UPDATE PUNTER_CATEGORY SET DISPLAY_NAME='Wiseguy' WHERE ID=1002;
    UPDATE PUNTER_CATEGORY SET DISPLAY_NAME='Arbitrage' WHERE ID=1003;
    UPDATE PUNTER_CATEGORY SET DISPLAY_NAME='Profitabel' WHERE ID=1004;
    UPDATE PUNTER_CATEGORY SET DISPLAY_NAME='Retail' WHERE ID=1005;
    COMMIT;
end;