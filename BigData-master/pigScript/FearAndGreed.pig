raw_data1 = LOAD '/Users/ukothan/Documents/workspace-assignment/StockMarketAnalysis/BigData/inputFiles/spxpc.csv' using PigStorage(',') AS (date:charArray,spPCRatio:double);
raw_data2 = LOAD '/Users/ukothan/Documents/workspace-assignment/StockMarketAnalysis/BigData/inputFiles/vixpc.csv' using PigStorage(',') AS (date:charArray,viPCRatio:double);
raw_data3 = LOAD '/Users/ukothan/Documents/workspace-assignment/StockMarketAnalysis/BigData/inputFiles/indexpc.csv' using PigStorage(',') AS (date:charArray,call:double,put:double,total:double,indexPCRatio:double);

fordata1 = FOREACH raw_data1 GENERATE ToDate(date,'M/dd/yy') as date1,spPCRatio;
fordata2 = FOREACH raw_data2 GENERATE ToDate(date,'M/dd/yy') as date2,viPCRatio;
fordata3 = FOREACH raw_data3 GENERATE ToDate(date,'M/dd/yy') as date3,indexPCRatio;
join_data1 = join fordata1 by date1, fordata2 by date2,fordata3 by date3;

forEachData = FOREACH join_data1 GENERATE date1,((((spPCRatio+viPCRatio+indexPCRatio)/3)/2)*100) as volatilityIndex;
STORE forEachData into '/Users/ukothan/Documents/workspace-assignment/StockMarketAnalysis/BigData/pigOutput/fearAndGreed' using PigStorage(',');