raw_data = LOAD 'hdfs://localhost:9000/hadoop/companyListUpdated.csv' using PigStorage(',') AS (symbol:chararray,name:chararray,lastSale:double,marketCap:double,IPOyear:int,Sector:chararray,industry:chararray);

STORE raw_data INTO 'hbase://companyList' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('companyData:name,companyData:lastSale,companyData:marketCap,companyData:IPOyear,companyData:Sector,companyData:industry');

