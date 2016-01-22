input_data = LOAD 'hdfs://localhost:9000/hadoop/output.csv' using PigStorage(',') AS (id:int,symbol:charArray,date:chararray,open:double,high:double,low:double,close:double,volume:int,adjClose:double);

symbol_data = FILTER input_data by symbol matches 'AAME';

forEachSymbolData = FOREACH symbol_data GENERATE org.apache.pig.builtin.GetYear(ToDate(date,'M/dd/yy')) as year,open,high,low,close,volume,adjClose;
STORE forEachSymbolData into 'Query1Output' using PigStorage(',');


