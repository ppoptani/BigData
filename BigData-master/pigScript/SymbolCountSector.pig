raw_data = LOAD 'hdfs://localhost:9000/hadoop/companyListUpdated.csv' using PigStorage(',') AS (symbol:chararray,name:chararray,lastSale:double,marketCap:double,IPOyear:int,Sector:chararray,industry:chararray);

groupSector = GROUP raw_data by Sector;

foreachSector = FOREACH groupSector GENERATE group,COUNT(raw_data.symbol);

STORE foreachSector into '/Users/ukothan/Documents/workspace-assignment/StockMarketAnalysis/BigData/pigOutput/SymbolCountSector' using PigStorage(',');

