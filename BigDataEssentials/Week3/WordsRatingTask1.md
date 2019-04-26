
# Hadoop Streaming assignment 1: Words Rating

The purpose of this task is to create your own WordCount program for Wikipedia dump processing and learn basic concepts of the MapReduce.

In this task you have to find the 7th word by popularity and its quantity in the reverse order (most popular first) in Wikipedia data (`/data/wiki/en_articles_part`).

There are several points for this task:

1) As an output, you have to get the 7th word and its quantity separated by a tab character.

2) You must use the second job to obtain a totally ordered result.

3) Do not forget to redirect all trash and output to /dev/null.

Here you can find the draft of the task main steps. You can use other methods for solution obtaining.

## Step 1. Create mapper and reducer.

<b>Hint:</b>  Demo task contains almost all the necessary pieces to complete this assignment. You may use the demo to implement the first MapReduce Job.


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


```python
%%writefile reducer.py

import sys

current_key = None
word_sum = 0

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

## Step 2. Create sort job.

<b>Hint:</b> You may use MapReduce comparator to solve this step. Make sure that the keys are sorted in ascending order.


```python
%%writefile mapper_rating.py

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


```python
%%writefile reducer_rating.py

import sys

for line in sys.stdin:
    try:
        count, word = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
        
    print "%s\t%d" % (word, count)
```

## Step 3. Bash commands

<b> Hint: </b> For printing the exact row you may use basic UNIX commands. For instance, sed/head/tail/... (if you know other commands, you can use them).

To run both jobs, you must use two consecutive yarn-commands. Remember that the input for the second job is the ouput for the first job.


```bash
%%bash

OUT_DIR="assignment1_"$(date +"%s%6N")

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

OUT_DIR_2="assignment1_2ndjob_"$(date +"%s%6N")
NUM_REDUCERS=1

hdfs dfs -rm -r -skipTrash ${OUT_DIR_2} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming wordCount Rating" \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D map.output.key.field.separator=\t \
    -D mapreduce.partition.keycomparator.options=-k1,1nr \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper_rating.py,reducer_rating.py \
    -mapper "python mapper_rating.py" \
    -reducer "python reducer_rating.py" \
    -input ${OUT_DIR} \
    -output ${OUT_DIR_2} > /dev/null

# Code for obtaining the results
hdfs dfs -cat ${OUT_DIR}/part-00000 | sed -n '7p;8q'
hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null
```
