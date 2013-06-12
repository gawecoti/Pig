register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- load the test file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-*' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by subject column
objects = group ntriples by (subject) PARALLEL 50;

-- flatten the objects out (because group by produces a tuple of each object
-- in the first column, and we want each object to be a string, not a tuple),
-- and count the number of tuples associated with each object
count_by_subject = foreach objects generate flatten($0), COUNT($1) as count PARALLEL 50;

-- group by count
hist_count = group count_by_subject by (count) PARALLEL 50;

-- flatten again
count_by_hist_count = foreach hist_count generate flatten($0), COUNT($1) as count PARALLEL 50;

-- store the results in the folder /user/hadoop/example-results
store count_by_hist_count into '/user/hadoop/example-results' using PigStorage();
