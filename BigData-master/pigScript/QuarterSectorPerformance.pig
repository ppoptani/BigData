register pig.jar;
raw_data = LOAD 'hdfs://localhost:9000/hadoop/output.csv' using PigStorage(',') AS (id:int,symbol:charArray,date:charArray,open:double,high:double,low:double,close:double,volume:int,adjClose:double);
weekExtractYear = FOREACH raw_data GENERATE symbol,ToDate(date,'M/dd/yy') as dateTime,date,open,high,low,close,volume,adjClose;
yearFilteredData = FILTER weekExtractYear BY GetWeekYear(val_0) == 2011;
quarterData= FOREACH yearFilteredData GENERATE symbol,pig.QuarterComputationPig(date) as Quarter,open,high,low,close,volume,adjClose;
filterQuarterData = FILTER quarterData by Quarter matches '1';
quarterGrpData = GROUP filterQuarterData by symbol;
quarterAvgData = FOREACH quarterGrpData GENERATE group as symbol,AVG(filterQuarterData.open) as avgOpen,AVG(filterQuarterData.high)as avgHigh,AVG(filterQuarterData.low) as avgLow,AVG(filterQuarterData.close) as avgClose,AVG( filterQuarterData.volume) as avgVolume,AVG(filterQuarterData.adjClose) as avgAdjClose;

raw_data1 = LOAD '/Users/ukothan/Documents/workspace-assignment/StockMarketAnalysis/BigData/inputFiles/companyListUpdated.csv' using PigStorage(',') AS (symbol:chararray,name:chararray,lastSale:double,marketCap:double,IPOyear:int,Sector:chararray);

joindData1 = JOIN raw_data1 by symbol,quarterAvgData by symbol;

groupData =  GROUP joindData1 by raw_data1.Sector as sector; 

forSectorData = FOREACH groupData GENERATE sector,AVG(groupData.open)AVG(groupData.high)AVG(groupData.low)AVG(groupData.close)AVG(groupData.volume),AVG(groupData.adjClose);

STORE quarterAvgData into 'quarterPerformance' using PigStorage(',');