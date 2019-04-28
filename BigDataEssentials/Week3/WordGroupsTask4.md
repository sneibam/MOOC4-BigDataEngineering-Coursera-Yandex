
## Assignment 4: Word Groups
Calculate statistics for groups of words which are equal up to permutations of letters. For example, ‘emit’, ‘item’ and ‘time’ are the same words up to a permutation of letters. Determine such groups of words and sum all their counts. Apply stop words filter. Filter out groups that consist of only one word.

Output: count of occurrences for the group of words, number of unique words in the group, comma-separated list of the words in the group in lexicographical order:
```
sum <tab> group size <tab> word1,word2,...
```
Example: assume ‘emit’ occurred 3 times, 'item' -- 2 times, 'time' -- 5 times; 3 + 2 + 5 = 10, group contains 3 words, so for this group result is:
```
10 3 emit,item,time
```
The result of the task is the output line with word ‘english’.

***NB:*** *Do not forget about the lexicographical order of words in the group: 'emit,item,time' is OK, 'emit,time,item' is not.*


```python
%%writefile mapper.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

with open('stop_words_en.txt') as f:
    stop_words = set(f.read().split())

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    for word in words:
        word = word.lower()
        if word in stop_words:
            continue
        word_sorted = ''.join(sorted(word))
        print "%s\t%d\t%s" % (word_sorted, 1, word)
```

    Overwriting mapper.py



```python
%%writefile reducer.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

current_key = None
current_cnt = 0
words_set = set()

for line in sys.stdin:
    try:
        key, cnt, word = unicode(line.strip()).split('\t')
        cnt = int(cnt)
    except ValueError as e:
        continue
    
    if current_key != key:
        if current_key and (len(words_set) > 1):
            print "%d\t%d\t%s" % (current_cnt, len(words_set), ','.join(sorted(words_set)))
        current_key = key
        words_set = set()
        words_set.add(word)
        current_cnt = cnt
    else:
        words_set.add(word)
        current_cnt += cnt
        
print "%d\t%d\t%s" % (current_cnt, len(words_set), ','.join(sorted(words_set)))
```

    Overwriting reducer.py



```bash
%%bash

OUT_DIR="word_groups"
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming word groups" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py,/datasets/stop_words_en.txt \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null
    
hdfs dfs -cat word_groups/* | grep -P '(,|\t)english($|,)'
```

    7820	5	english,helsing,hesling,shengli,shingle


    rm: `word_groups': No such file or directory
    19/04/28 12:03:23 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/28 12:03:23 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/28 12:03:23 INFO mapred.FileInputFormat: Total input files to process : 1
    19/04/28 12:03:23 INFO mapreduce.JobSubmitter: number of splits:2
    19/04/28 12:03:24 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1556403047654_0007
    19/04/28 12:03:24 INFO impl.YarnClientImpl: Submitted application application_1556403047654_0007
    19/04/28 12:03:24 INFO mapreduce.Job: The url to track the job: http://7522552313b7:8088/proxy/application_1556403047654_0007/
    19/04/28 12:03:24 INFO mapreduce.Job: Running job: job_1556403047654_0007
    19/04/28 12:03:30 INFO mapreduce.Job: Job job_1556403047654_0007 running in uber mode : false
    19/04/28 12:03:30 INFO mapreduce.Job:  map 0% reduce 0%
    19/04/28 12:03:46 INFO mapreduce.Job:  map 48% reduce 0%
    19/04/28 12:03:52 INFO mapreduce.Job:  map 66% reduce 0%
    19/04/28 12:03:55 INFO mapreduce.Job:  map 82% reduce 0%
    19/04/28 12:03:56 INFO mapreduce.Job:  map 100% reduce 0%
    19/04/28 12:04:04 INFO mapreduce.Job:  map 100% reduce 13%
    19/04/28 12:04:05 INFO mapreduce.Job:  map 100% reduce 25%
    19/04/28 12:04:06 INFO mapreduce.Job:  map 100% reduce 38%
    19/04/28 12:04:07 INFO mapreduce.Job:  map 100% reduce 50%
    19/04/28 12:04:08 INFO mapreduce.Job:  map 100% reduce 63%
    19/04/28 12:04:09 INFO mapreduce.Job:  map 100% reduce 75%
    19/04/28 12:04:10 INFO mapreduce.Job:  map 100% reduce 88%
    19/04/28 12:04:12 INFO mapreduce.Job:  map 100% reduce 100%
    19/04/28 12:04:12 INFO mapreduce.Job: Job job_1556403047654_0007 completed successfully
    19/04/28 12:04:12 INFO mapreduce.Job: Counters: 50
    	File System Counters
    		FILE: Number of bytes read=272514475
    		FILE: Number of bytes written=410076142
    		FILE: Number of read operations=0
    		FILE: Number of large read operations=0
    		FILE: Number of write operations=0
    		HDFS: Number of bytes read=76874501
    		HDFS: Number of bytes written=772703
    		HDFS: Number of read operations=30
    		HDFS: Number of large read operations=0
    		HDFS: Number of write operations=16
    	Job Counters 
    		Killed reduce tasks=1
    		Launched map tasks=2
    		Launched reduce tasks=8
    		Data-local map tasks=2
    		Total time spent by all maps in occupied slots (ms)=46927
    		Total time spent by all reduces in occupied slots (ms)=43850
    		Total time spent by all map tasks (ms)=46927
    		Total time spent by all reduce tasks (ms)=43850
    		Total vcore-milliseconds taken by all map tasks=46927
    		Total vcore-milliseconds taken by all reduce tasks=43850
    		Total megabyte-milliseconds taken by all map tasks=48053248
    		Total megabyte-milliseconds taken by all reduce tasks=44902400
    	Map-Reduce Framework
    		Map input records=4100
    		Map output records=6971114
    		Map output bytes=122283220
    		Map output materialized bytes=136225566
    		Input split bytes=228
    		Combine input records=0
    		Combine output records=0
    		Reduce input groups=370340
    		Reduce shuffle bytes=136225566
    		Reduce input records=6971114
    		Reduce output records=32580
    		Spilled Records=20913342
    		Shuffled Maps =16
    		Failed Shuffles=0
    		Merged Map outputs=16
    		GC time elapsed (ms)=807
    		CPU time spent (ms)=83070
    		Physical memory (bytes) snapshot=2235871232
    		Virtual memory (bytes) snapshot=20220235776
    		Total committed heap usage (bytes)=1533542400
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
    		Bytes Written=772703
    19/04/28 12:04:12 INFO streaming.StreamJob: Output directory: word_groups

