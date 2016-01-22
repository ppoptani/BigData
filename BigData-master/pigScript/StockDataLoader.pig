raw_data = LOAD 'hdfs://ip-172-31-4-188.ec2.internal:9000/hadoop/output.csv' using PigStorage(',') AS (id:int,symbol:charArray,date:charArray,open:double,high:double,low:double,close:double,volume:int,adjClose:double);

STORE raw_data INTO 'hbase://stocks' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('stockData:symbol stockData:date stockPriceData:open stockPriceData:high stockPriceData:low stockPriceData:close stockPriceData:volume stockPriceData:adjClose');

hbase_data = load 'hbase://stocks' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'stockData:symbol,stockData:date,stockPriceData:*','-loadKey true')
 as (id,symbol:chararray,date:chararray,stockPriceData:map[]);

 



