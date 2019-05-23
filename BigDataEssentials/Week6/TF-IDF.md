

```python
%%writefile mapper.py

from __future__ import division
import sys
import re
from collections import Counter
```

    Overwriting mapper.py



```python
%%writefile -a mapper.py

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

with open('stop_words_en.txt') as f:
    stop_words = set(f.read().split())

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
        article_id = int(article_id)
    except ValueError as e:
        continue
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    words = [word.lower() for word in words if word.lower() not in stop_words]
    words_set = set(words)
    
    num_of_words_in_doc = len(words)
    counts = Counter(words)
    
    for word in words_set:
        num_of_word_in_doc = counts[word]
        tf = num_of_word_in_doc / num_of_words_in_doc
        print "%s\t%d\t%f" % (word, article_id, tf)
```

    Appending to mapper.py



```python
%%writefile reducer.py

from __future__ import division
import sys
from math import log

current_word = None
articles_dict = dict()

for line in sys.stdin:
    try:
        word, article_id, tf = line.strip().split('\t')
        article_id = int(article_id)
        tf = float(tf)
    except ValueError as e:
        continue
    
    if current_word != word:
        if current_word:
            idf = 1 / log(1 + len(articles_dict))
            for key_article_id, tf in articles_dict.iteritems():
                tfidf = tf * idf
                print "%s\t%d\t%f" % (current_word, key_article_id, tfidf)
        articles_dict = dict()
        current_word = word
    articles_dict[article_id] = tf

if current_word:
    print "%s\t%d\t%f" % (current_word, article_id, tfidf)
```

    Overwriting reducer.py



```bash
%%bash

OUT_DIR="tf_idf"
NUM_REDUCERS=5

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="TF_IDF" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py,/datasets/stop_words_en.txt \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null

hdfs dfs -cat tf_idf/* | grep -P 'labor\t12\t' | cut -f3
```

    labor	12	0.000351


    19/05/23 11:33:53 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/05/23 11:33:53 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/05/23 11:33:53 INFO mapred.FileInputFormat: Total input files to process : 1
    19/05/23 11:33:53 INFO mapreduce.JobSubmitter: number of splits:2
    19/05/23 11:33:54 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1558573847728_0008
    19/05/23 11:33:54 INFO impl.YarnClientImpl: Submitted application application_1558573847728_0008
    19/05/23 11:33:54 INFO mapreduce.Job: The url to track the job: http://e0249d2b3fbb:8088/proxy/application_1558573847728_0008/
    19/05/23 11:33:54 INFO mapreduce.Job: Running job: job_1558573847728_0008
    19/05/23 11:34:00 INFO mapreduce.Job: Job job_1558573847728_0008 running in uber mode : false
    19/05/23 11:34:00 INFO mapreduce.Job:  map 0% reduce 0%
    19/05/23 11:34:20 INFO mapreduce.Job:  map 66% reduce 0%
    19/05/23 11:34:21 INFO mapreduce.Job:  map 83% reduce 0%
    19/05/23 11:34:23 INFO mapreduce.Job:  map 100% reduce 0%
    19/05/23 11:34:36 INFO mapreduce.Job:  map 100% reduce 40%
    19/05/23 11:34:39 INFO mapreduce.Job:  map 100% reduce 100%
    19/05/23 11:34:39 INFO mapreduce.Job: Job job_1558573847728_0008 completed successfully
    19/05/23 11:34:39 INFO mapreduce.Job: Counters: 50
    	File System Counters
    		FILE: Number of bytes read=83708118
    		FILE: Number of bytes written=168395700
    		FILE: Number of read operations=0
    		FILE: Number of large read operations=0
    		FILE: Number of write operations=0
    		HDFS: Number of bytes read=76874501
    		HDFS: Number of bytes written=76761713
    		HDFS: Number of read operations=21
    		HDFS: Number of large read operations=0
    		HDFS: Number of write operations=10
    	Job Counters 
    		Killed reduce tasks=1
    		Launched map tasks=2
    		Launched reduce tasks=6
    		Data-local map tasks=2
    		Total time spent by all maps in occupied slots (ms)=33729
    		Total time spent by all reduces in occupied slots (ms)=56141
    		Total time spent by all map tasks (ms)=33729
    		Total time spent by all reduce tasks (ms)=56141
    		Total vcore-milliseconds taken by all map tasks=33729
    		Total vcore-milliseconds taken by all reduce tasks=56141
    		Total megabyte-milliseconds taken by all map tasks=34538496
    		Total megabyte-milliseconds taken by all reduce tasks=57488384
    	Map-Reduce Framework
    		Map input records=4100
    		Map output records=3472743
    		Map output bytes=76762592
    		Map output materialized bytes=83708148
    		Input split bytes=228
    		Combine input records=0
    		Combine output records=0
    		Reduce input groups=426865
    		Reduce shuffle bytes=83708148
    		Reduce input records=3472743
    		Reduce output records=3472685
    		Spilled Records=6945486
    		Shuffled Maps =10
    		Failed Shuffles=0
    		Merged Map outputs=10
    		GC time elapsed (ms)=547
    		CPU time spent (ms)=44960
    		Physical memory (bytes) snapshot=1570615296
    		Virtual memory (bytes) snapshot=14127251456
    		Total committed heap usage (bytes)=1098907648
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
    		Bytes Written=76761713
    19/05/23 11:34:39 INFO streaming.StreamJob: Output directory: tf_idf

