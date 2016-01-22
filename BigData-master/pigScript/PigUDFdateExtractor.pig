register pig.jar;
raw_data = LOAD 'hdfs://localhost:9000/hadoop/output.csv' using PigStorage(',') AS (id:int,symbol:charArray,date:charArray,open:double,high:double,low:double,close:double,volume:int,adjClose:double);
weekYearData = FOREACH raw_data GENERATE symbol,pig.WeekExtractorUDF(date) as processedDate,open,high,low,close,volume,adjClose;
weekParticularData = FILTER weekYearData BY processedDate matches '12,2001';

weekParticularGrpData=GROUP weekParticularData by symbol;

weekParticularGrpData = GROUP weekParticularData by symbol;

//weekParticularAvgData = FOREACH weekParticularGrpData GENERATE group as symbol,AVG(weekParticularData.open) as avgOpen,AVG(weekParticularData.high)as avgHigh,AVG(weekParticularData.low) as avgLow,AVG(weekParticularData.close) as avgClose,AVG(weekParticularData.volume) as avgVolume,AVG(weekParticularData.adjClose) as avgAdjClose;