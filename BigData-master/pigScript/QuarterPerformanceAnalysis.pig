register pig.jar;
raw_data = LOAD 'hdfs://ip-172-31-4-188.ec2.internal:9000/hadoop/output.csv' using PigStorage(',') AS (id:int,symbol:charArray,date:charArray,open:double,high:double,low:double,close:double,volume:int,adjClose:double);
weekExtractYear = FOREACH raw_data GENERATE symbol,ToDate(date,'M/dd/yy') as dateTime,date,open,high,low,close,volume,adjClose;
yearFilteredData = FILTER weekExtractYear BY GetWeekYear(val_0) == 2011;
quarterData= FOREACH yearFilteredData GENERATE symbol,pig.QuarterComputationPig(date) as Quarter,open,high,low,close,volume,adjClose;
filterQuarterData = FILTER quarterData by Quarter matches '1';
quarterGrpData = GROUP filterQuarterData by symbol;
quarterAvgData = FOREACH quarterGrpData GENERATE group as symbol,AVG(filterQuarterData.open) as avgOpen,AVG(filterQuarterData.high)as avgHigh,AVG(filterQuarterData.low) as avgLow,AVG(filterQuarterData.close) as avgClose,AVG( filterQuarterData.volume) as avgVolume,AVG(filterQuarterData.adjClose) as avgAdjClose;
STORE quarterAvgData into 'quarterPerformance' using PigStorage(',');