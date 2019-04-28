
## Assignment 3: Name Count

Make WordCount program for all the names in the dataset. Name is a word with the following properties:

The first character is not a digit (other characters can be digits).
The first character is uppercase, all the other characters that are letters are lowercase.
There are less than 0.5% occurrences of this word, when this word regardless to its case appears in the dataset and the condition (2) is not met. 

Order by quantity, most popular first, output format:

`name <tab> count`

The result is the 5th line in the output, for example:

`john 1234`


```python
%%writefile mapper.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

def is_name(word):
    if len(word) < 2:
        return False
    elif (word[0].isalpha()) and (word[0].isupper()) and (word[1:].islower()):
        return True
    else:
        return False

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    for word in words:
        name_flag = int(is_name(word))
        print "%s\t%d\t%d" % (word.lower(), 1, name_flag)
```

    Writing mapper.py



```python
%%writefile reducer.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

def condition_name(cnt, cnt_name):
    if (cnt - cnt_name) / float(cnt) * 100 < 0.5:
        return True
    else:
        return False

current_key = None
current_cnt = 0
current_cnt_name = 0

for line in sys.stdin:
    try:
        key, cnt, cnt_name = unicode(line.strip()).split('\t')
        cnt = int(cnt)
        cnt_name = int(cnt_name)
    except ValueError as e:
        continue
    
    if current_key != key:
        if current_key and condition_name(current_cnt, current_cnt_name):
            print "%s\t%d" % (current_key, current_cnt)
        current_key = key
        current_cnt = cnt
        current_cnt_name = cnt_name
    else:
        current_cnt += cnt
        current_cnt_name += cnt_name
        
print "%s\t%d" % (current_key, current_cnt)
```

    Writing reducer.py



```python
%%writefile mapper_sort.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

for line in sys.stdin:
    try:
        word, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
        
    print "%d\t%s" % (count, word)
```

    Writing mapper_sort.py



```python
%%writefile reducer_sort.py

import sys

for line in sys.stdin:
    try:
        count, word = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
        
    print "%s\t%d" % (word, count)
```

    Writing reducer_sort.py



```bash
%%bash

OUT_DIR="name_count_task"$(date +"%s%6N")
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming name count" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null
    
OUT_DIR_2="name_count_task2"$(date +"%s%6N")
NUM_REDUCERS=1

hdfs dfs -rm -r -skipTrash ${OUT_DIR_2} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming name count sorting" \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D map.output.key.field.separator=\t \
    -D mapreduce.partition.keycomparator.options=-k1,1nr \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper_sort.py,reducer_sort.py \
    -mapper "python mapper_sort.py" \
    -reducer "python reducer_sort.py" \
    -input ${OUT_DIR} \
    -output ${OUT_DIR_2} > /dev/null
    
hdfs dfs -cat ${OUT_DIR_2}/part-00000 | head -5 | tail -1
```

    french	5753


    rm: `name_count_task1556449328435380': No such file or directory
    19/04/28 11:02:12 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/28 11:02:12 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/28 11:02:13 INFO mapred.FileInputFormat: Total input files to process : 1
    19/04/28 11:02:14 INFO mapreduce.JobSubmitter: number of splits:2
    19/04/28 11:02:14 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1556403047654_0005
    19/04/28 11:02:14 INFO impl.YarnClientImpl: Submitted application application_1556403047654_0005
    19/04/28 11:02:14 INFO mapreduce.Job: The url to track the job: http://7522552313b7:8088/proxy/application_1556403047654_0005/
    19/04/28 11:02:14 INFO mapreduce.Job: Running job: job_1556403047654_0005
    19/04/28 11:02:20 INFO mapreduce.Job: Job job_1556403047654_0005 running in uber mode : false
    19/04/28 11:02:20 INFO mapreduce.Job:  map 0% reduce 0%
    19/04/28 11:02:37 INFO mapreduce.Job:  map 41% reduce 0%
    19/04/28 11:02:43 INFO mapreduce.Job:  map 54% reduce 0%
    19/04/28 11:02:49 INFO mapreduce.Job:  map 67% reduce 0%
    19/04/28 11:02:53 INFO mapreduce.Job:  map 83% reduce 0%
    19/04/28 11:02:54 INFO mapreduce.Job:  map 100% reduce 0%
    19/04/28 11:03:02 INFO mapreduce.Job:  map 100% reduce 13%
    19/04/28 11:03:03 INFO mapreduce.Job:  map 100% reduce 25%
    19/04/28 11:03:06 INFO mapreduce.Job:  map 100% reduce 63%
    19/04/28 11:03:08 INFO mapreduce.Job:  map 100% reduce 75%
    19/04/28 11:03:11 INFO mapreduce.Job:  map 100% reduce 88%
    19/04/28 11:03:12 INFO mapreduce.Job:  map 100% reduce 100%
    19/04/28 11:03:13 INFO mapreduce.Job: Job job_1556403047654_0005 completed successfully
    19/04/28 11:03:13 INFO mapreduce.Job: Counters: 50
    	File System Counters
    		FILE: Number of bytes read=291239559
    		FILE: Number of bytes written=438172640
    		FILE: Number of read operations=0
    		FILE: Number of large read operations=0
    		FILE: Number of write operations=0
    		HDFS: Number of bytes read=76874501
    		HDFS: Number of bytes written=1509305
    		HDFS: Number of read operations=30
    		HDFS: Number of large read operations=0
    		HDFS: Number of write operations=16
    	Job Counters 
    		Killed reduce tasks=1
    		Launched map tasks=2
    		Launched reduce tasks=9
    		Data-local map tasks=2
    		Total time spent by all maps in occupied slots (ms)=60664
    		Total time spent by all reduces in occupied slots (ms)=66908
    		Total time spent by all map tasks (ms)=60664
    		Total time spent by all reduce tasks (ms)=66908
    		Total vcore-milliseconds taken by all map tasks=60664
    		Total vcore-milliseconds taken by all reduce tasks=66908
    		Total megabyte-milliseconds taken by all map tasks=62119936
    		Total megabyte-milliseconds taken by all reduce tasks=68513792
    	Map-Reduce Framework
    		Map input records=4100
    		Map output records=11937375
    		Map output bytes=121717186
    		Map output materialized bytes=145592042
    		Input split bytes=228
    		Combine input records=0
    		Combine output records=0
    		Reduce input groups=427176
    		Reduce shuffle bytes=145592042
    		Reduce input records=11937375
    		Reduce output records=135356
    		Spilled Records=35812125
    		Shuffled Maps =16
    		Failed Shuffles=0
    		Merged Map outputs=16
    		GC time elapsed (ms)=1506
    		CPU time spent (ms)=102700
    		Physical memory (bytes) snapshot=2292379648
    		Virtual memory (bytes) snapshot=20236460032
    		Total committed heap usage (bytes)=1496317952
    	Shuffle Errors
    		BAD_ID=0
    		CONNECTION=0
    		IO_ERROR=0
    		WRONG_LENGTH=0
    		WRONG_MAP=0
    		WRONG_REDUCE=0
    	File Input Format Counters 
    		Bytes Read=76874273
    	File Output Format Counters 
    		Bytes Written=1509305
    19/04/28 11:03:13 INFO streaming.StreamJob: Output directory: name_count_task1556449328435380
    rm: `name_count_task21556449393718432': No such file or directory
    19/04/28 11:03:17 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/28 11:03:17 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/28 11:03:17 INFO mapred.FileInputFormat: Total input files to process : 8
    19/04/28 11:03:18 INFO mapreduce.JobSubmitter: number of splits:8
    19/04/28 11:03:18 INFO Configuration.deprecation: map.output.key.field.separator is deprecated. Instead, use mapreduce.map.output.key.field.separator
    19/04/28 11:03:18 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1556403047654_0006
    19/04/28 11:03:19 INFO impl.YarnClientImpl: Submitted application application_1556403047654_0006
    19/04/28 11:03:19 INFO mapreduce.Job: The url to track the job: http://7522552313b7:8088/proxy/application_1556403047654_0006/
    19/04/28 11:03:19 INFO mapreduce.Job: Running job: job_1556403047654_0006
    19/04/28 11:03:25 INFO mapreduce.Job: Job job_1556403047654_0006 running in uber mode : false
    19/04/28 11:03:25 INFO mapreduce.Job:  map 0% reduce 0%
    19/04/28 11:03:30 INFO mapreduce.Job:  map 75% reduce 0%
    19/04/28 11:03:33 INFO mapreduce.Job:  map 100% reduce 0%
    19/04/28 11:03:36 INFO mapreduce.Job:  map 100% reduce 100%
    19/04/28 11:03:37 INFO mapreduce.Job: Job job_1556403047654_0006 completed successfully
    19/04/28 11:03:37 INFO mapreduce.Job: Counters: 49
    	File System Counters
    		FILE: Number of bytes read=1780025
    		FILE: Number of bytes written=4822341
    		FILE: Number of read operations=0
    		FILE: Number of large read operations=0
    		FILE: Number of write operations=0
    		HDFS: Number of bytes read=1510329
    		HDFS: Number of bytes written=1509305
    		HDFS: Number of read operations=27
    		HDFS: Number of large read operations=0
    		HDFS: Number of write operations=2
    	Job Counters 
    		Launched map tasks=8
    		Launched reduce tasks=1
    		Data-local map tasks=8
    		Total time spent by all maps in occupied slots (ms)=25208
    		Total time spent by all reduces in occupied slots (ms)=3134
    		Total time spent by all map tasks (ms)=25208
    		Total time spent by all reduce tasks (ms)=3134
    		Total vcore-milliseconds taken by all map tasks=25208
    		Total vcore-milliseconds taken by all reduce tasks=3134
    		Total megabyte-milliseconds taken by all map tasks=25812992
    		Total megabyte-milliseconds taken by all reduce tasks=3209216
    	Map-Reduce Framework
    		Map input records=135356
    		Map output records=135356
    		Map output bytes=1509306
    		Map output materialized bytes=1780067
    		Input split bytes=1024
    		Combine input records=0
    		Combine output records=0
    		Reduce input groups=580
    		Reduce shuffle bytes=1780067
    		Reduce input records=135356
    		Reduce output records=135356
    		Spilled Records=270712
    		Shuffled Maps =8
    		Failed Shuffles=0
    		Merged Map outputs=8
    		GC time elapsed (ms)=1354
    		CPU time spent (ms)=12980
    		Physical memory (bytes) snapshot=2487853056
    		Virtual memory (bytes) snapshot=18104868864
    		Total committed heap usage (bytes)=1748500480
    	Shuffle Errors
    		BAD_ID=0
    		CONNECTION=0
    		IO_ERROR=0
    		WRONG_LENGTH=0
    		WRONG_MAP=0
    		WRONG_REDUCE=0
    	File Input Format Counters 
    		Bytes Read=1509305
    	File Output Format Counters 
    		Bytes Written=1509305
    19/04/28 11:03:37 INFO streaming.StreamJob: Output directory: name_count_task21556449393718432
    cat: Unable to write to output stream.

