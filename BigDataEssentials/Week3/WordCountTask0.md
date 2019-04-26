

```python
%%writefile mapper.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    for word in words:
        print >> sys.stderr, "reporter:counter:Wiki stats,Total words,%d" % 1
        print "%s\t%d" % (word.lower(), 1)
```

    Writing mapper.py



```python
%%writefile reducer.py

import sys

current_key = None
word_sum = 0
```

    Writing reducer.py



```python
%%writefile -a reducer.py

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
            print "%s\t%d" % (current_key, word_sum)
        word_sum = 0
        current_key = key
    word_sum += count

if current_key:
    print "%s\t%d" % (current_key, word_sum)
```

    Appending to reducer.py



```python
! hdfs dfs -ls /data/wiki
```

    Found 4 items
    drwxr-xr-x   - hdfs supergroup          0 2017-07-03 23:21 /data/wiki/en_articles
    drwxr-xr-x   - hdfs supergroup          0 2017-07-03 22:37 /data/wiki/en_articles_part
    drwxr-xr-x   - hdfs supergroup          0 2017-08-11 04:12 /data/wiki/en_articles_part1
    -rw-r--r--   2 hdfs supergroup       1914 2017-08-04 16:12 /data/wiki/stop_words_en-xpo6.txt



```bash
%%bash

OUT_DIR="wordcount_result_"$(date +"%s%6N")
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming wordCount" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python mapper.py" \
    -combiner "python reducer.py" \
    -reducer "python reducer.py" \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null

hdfs dfs -cat ${OUT_DIR}/part-00000 | head
```

    0%however	1
    0&\mathrm{if	1
    0(8)320-1234	1
    0)).(1	2
    0,03	1
    0,1,...,n	1
    0,1,0	1
    0,1,\dots,n	1
    0,5	1
    0,50	1


    rm: `wordcount_result_1504600278440756': No such file or directory
    17/09/05 12:31:23 INFO client.RMProxy: Connecting to ResourceManager at mipt-master.atp-fivt.org/93.175.29.106:8032
    17/09/05 12:31:23 INFO client.RMProxy: Connecting to ResourceManager at mipt-master.atp-fivt.org/93.175.29.106:8032
    17/09/05 12:31:24 INFO mapred.FileInputFormat: Total input paths to process : 1
    17/09/05 12:31:24 INFO mapreduce.JobSubmitter: number of splits:3
    17/09/05 12:31:24 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1503302131685_0379
    17/09/05 12:31:25 INFO impl.YarnClientImpl: Submitted application application_1503302131685_0379
    17/09/05 12:31:25 INFO mapreduce.Job: The url to track the job: http://mipt-master.atp-fivt.org:8088/proxy/application_1503302131685_0379/
    17/09/05 12:31:25 INFO mapreduce.Job: Running job: job_1503302131685_0379
    17/09/05 12:31:31 INFO mapreduce.Job: Job job_1503302131685_0379 running in uber mode : false
    17/09/05 12:31:31 INFO mapreduce.Job:  map 0% reduce 0%
    17/09/05 12:31:47 INFO mapreduce.Job:  map 33% reduce 0%
    17/09/05 12:31:49 INFO mapreduce.Job:  map 55% reduce 0%
    17/09/05 12:31:55 INFO mapreduce.Job:  map 67% reduce 0%
    17/09/05 12:32:01 INFO mapreduce.Job:  map 78% reduce 0%
    17/09/05 12:32:11 INFO mapreduce.Job:  map 100% reduce 0%
    17/09/05 12:32:19 INFO mapreduce.Job:  map 100% reduce 100%
    17/09/05 12:32:19 INFO mapreduce.Job: Job job_1503302131685_0379 completed successfully
    17/09/05 12:32:19 INFO mapreduce.Job: Counters: 54
    	File System Counters
    		FILE: Number of bytes read=4851476
    		FILE: Number of bytes written=11925190
    		FILE: Number of read operations=0
    		FILE: Number of large read operations=0
    		FILE: Number of write operations=0
    		HDFS: Number of bytes read=76993447
    		HDFS: Number of bytes written=5370513
    		HDFS: Number of read operations=33
    		HDFS: Number of large read operations=0
    		HDFS: Number of write operations=16
    	Job Counters 
    		Launched map tasks=3
    		Launched reduce tasks=8
    		Rack-local map tasks=3
    		Total time spent by all maps in occupied slots (ms)=351040
    		Total time spent by all reduces in occupied slots (ms)=231588
    		Total time spent by all map tasks (ms)=87760
    		Total time spent by all reduce tasks (ms)=38598
    		Total vcore-milliseconds taken by all map tasks=87760
    		Total vcore-milliseconds taken by all reduce tasks=38598
    		Total megabyte-milliseconds taken by all map tasks=179732480
    		Total megabyte-milliseconds taken by all reduce tasks=118573056
    	Map-Reduce Framework
    		Map input records=4100
    		Map output records=11937375
    		Map output bytes=97842436
    		Map output materialized bytes=5627293
    		Input split bytes=390
    		Combine input records=11937375
    		Combine output records=575818
    		Reduce input groups=427175
    		Reduce shuffle bytes=5627293
    		Reduce input records=575818
    		Reduce output records=427175
    		Spilled Records=1151636
    		Shuffled Maps =24
    		Failed Shuffles=0
    		Merged Map outputs=24
    		GC time elapsed (ms)=1453
    		CPU time spent (ms)=126530
    		Physical memory (bytes) snapshot=4855193600
    		Virtual memory (bytes) snapshot=32990945280
    		Total committed heap usage (bytes)=7536115712
    		Peak Map Physical memory (bytes)=906506240
    		Peak Map Virtual memory (bytes)=2205544448
    		Peak Reduce Physical memory (bytes)=281800704
    		Peak Reduce Virtual memory (bytes)=3315249152
    	Shuffle Errors
    		BAD_ID=0
    		CONNECTION=0
    		IO_ERROR=0
    		WRONG_LENGTH=0
    		WRONG_MAP=0
    		WRONG_REDUCE=0
    	Wiki stats
    		Total words=11937375
    	File Input Format Counters 
    		Bytes Read=76993057
    	File Output Format Counters 
    		Bytes Written=5370513
    17/09/05 12:32:19 INFO streaming.StreamJob: Output directory: wordcount_result_1504600278440756
    cat: Unable to write to output stream.

