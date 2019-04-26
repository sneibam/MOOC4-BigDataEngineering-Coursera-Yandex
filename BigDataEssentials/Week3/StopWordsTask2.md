
# Hadoop Streaming assignment 2: Stop Words

The purpose of this task is to improve the previous "Word rating" program. You have to calculate how many stop words are there in the input dataset. Stop words list is in `/datasets/stop_words_en.txt` file. 

Use Hadoop counters to compute the number of stop words and total words in the dataset. The result is the percentage of stop words in the entire dataset (without percent symbol).

There are several points for this task:

1) As an output, you have to get the percentage of stop words in the entire dataset without percent symbol (correct answer on sample dataset is `41.603`).

2) As you can see in the Hadoop Streaming userguide "you will need to use `-files` option to tell the framework to pack your executable files as a part of a job submission."

3) Do not forget to redirect junk output to `/dev/null`.

4) You may modify mappers/reducers from "Word rating" task and parse its output to get the answer on "Stop Words" task.

5) You may use mapper/reducer to get `"Stop Words"` and `"Total Words"` amounts and redirect them to sys.stderr. After that you may redirect the output of MapReduce to the parsed function. In this function you may find rows correspond to these amounts and compute the percentage.

Here you can find the draft for the main steps of the task. You can use other methods to get the solution.

## Step 1. Create the mapper.

<b>Hint:</b> Create the mapper, which calculates Total word and Stop word amounts. You may redirect this information to sys.stderr. This will make it possible to parse these data on the next steps.

Example of the redirections:

`print >> sys.stderr, "reporter:counter:Wiki stats,Total words,%d" % count`

Remember about the Distributed cache. If we add option `-files mapper.py,reducer.py,/datasets/stop_words_en.txt`, then `mapper.py, reducer.py` and `stop_words_en.txt` file will be in the same directory on the datanodes. Hence, it is necessary to use a relative path `stop_words_en.txt` from the mapper to access this txt file.


```python
%%writefile mapper.py


import sys
import re


reload(sys)
sys.setdefaultencoding('utf-8')  # required to convert to unicode

path = 'stop_words_en.txt'

# Your code for reading stop words here

with open(path) as stop_words_file:
    stop_words = set(stop_words_file.read().split())

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue

    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)

    # Your code for mapper here.
    for word in words:
        print >> sys.stderr, "reporter:counter:wiki,total_words,%d" % 1
        if word in stop_words:
            print >> sys.stderr, "reporter:counter:wiki,stop_words,%d" % 1
        print "%s\t%d" % (word.lower(), 1)
```

    Overwriting mapper.py


## Step 2. Create the reducer.

Create the reducer, which will accumulate the information after the mapper step. You may implement the combiner if you want. It can be useful from optimizing and speed up your computations (see the lectures from the Week 2 for more details).


```python
%%writefile reducer.py

# Your code for reducer here.
```

    Overwriting reducer.py


## Step 3. Create the parsed function.

<b>Hint:</b> Create the function, which will parse MapReduce sys.stderr for Total word and Stop word amounts.

The `./counter_process.py` script should do the following:

- parse hadoop logs from Stderr,

- retrieve values of 2 user-defined counters,

- compute percentage and output it into the stdout.


```python
%%writefile counter_process.py

#! /usr/bin/env python

import sys
import re

output_log = list(map(lambda x: x.strip(), sys.stdin.read().split()))

pattern_tot = 'total_words='
regexp_tot = re.compile(pattern_tot)

pattern_stop = 'stop_words='
regexp_stop = re.compile(pattern_stop)

total_words = [int(x.replace(pattern_tot, '')) for x in output_log if regexp_tot.search(x)][0]
stop_words = [int(x.replace(pattern_stop, '')) for x in output_log if regexp_stop.search(x)][0]

print((stop_words / total_words) * 100)
```

    Overwriting counter_process.py


## Step 4. Bash commands

<b> Hints: </b> 

1) If you want to redirect standard output to txt file you may use the following argument in yarn jar:

```
yarn ... \
  ... \
  -output ${OUT_DIR} > /dev/null 2> $LOGS
```

2) For printing the percentage of stop words in the entire dataset you may parse the MapReduce output. Parsed script may be written in Python code. 

To get the result you may use the UNIX pipe operator `|`. The output of the first command acts as an input to the second command (see lecture file-content-exploration-2 for more details).

With this operator you may use command `cat` to redirect the output of MapReduce to ./counter_process.py with arguments, which correspond to the `"Stop words"` and `"Total words"` counters. Example is the following:

`cat $LOGS | python ./counter_process.py "Stop words" "Total words"`

Now something about Hadoop counters naming. 
 - Built-in Hadoop counters usually have UPPER_CASE names. To make the grading system possible to distinguish your custom counters and system ones please use the following pattern for their naming: `[Aa]aaa...` (all except the first letters should be in lowercase);
 - Another points is how Hadoop sorts the counters. It sorts them lexicographically. Grading system reads your first counter as Stop words counter and the second as Total words. Please name you counters in such way that Hadoop set the Stop words counter before the Total words. 
 
E.g. "Stop words" and "Total words" names are Ok because they correspond both requirements.

3) In Python code sys.argv is a list, which contains the command-line arguments passed to the script. The name of the script is in `sys.argv[0]`. Other arguments begin from `sys.argv[1]`.

Hence, if you have two arguments, which you send from the Bash to your python script, you may use arguments in your script with the following command:

`function(sys.argv[1], sys.argv[2])`

4) Do not forget about printing your MapReduce output in the last cell. You may use the next command:

`cat $LOGS >&2`


```bash
%%bash

OUT_DIR="coursera_mr_task2"$(date +"%s%6N")
NUM_REDUCERS=0
LOGS="stderr_logs.txt"

# Stub code for your job

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming stopwords" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,/datasets/stop_words_en.txt \
    -mapper "python mapper.py" \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null 2> $LOGS
    
cat $LOGS | python ./counter_process.py "Stop words" "Total words"
cat $LOGS >&2

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

```

    38.44036900909957


    rm: `coursera_mr_task21556317409442921': No such file or directory
    19/04/26 22:23:33 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/26 22:23:33 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
    19/04/26 22:23:33 INFO mapred.FileInputFormat: Total input files to process : 1
    19/04/26 22:23:34 INFO mapreduce.JobSubmitter: number of splits:2
    19/04/26 22:23:34 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1556307047318_0006
    19/04/26 22:23:34 INFO impl.YarnClientImpl: Submitted application application_1556307047318_0006
    19/04/26 22:23:34 INFO mapreduce.Job: The url to track the job: http://659e12a29d9b:8088/proxy/application_1556307047318_0006/
    19/04/26 22:23:34 INFO mapreduce.Job: Running job: job_1556307047318_0006
    19/04/26 22:23:40 INFO mapreduce.Job: Job job_1556307047318_0006 running in uber mode : false
    19/04/26 22:23:40 INFO mapreduce.Job:  map 0% reduce 0%
    19/04/26 22:23:56 INFO mapreduce.Job:  map 37% reduce 0%
    19/04/26 22:24:02 INFO mapreduce.Job:  map 56% reduce 0%
    19/04/26 22:24:09 INFO mapreduce.Job:  map 76% reduce 0%
    19/04/26 22:24:15 INFO mapreduce.Job:  map 95% reduce 0%
    19/04/26 22:24:16 INFO mapreduce.Job:  map 96% reduce 0%
    19/04/26 22:24:18 INFO mapreduce.Job:  map 100% reduce 0%
    19/04/26 22:24:19 INFO mapreduce.Job: Job job_1556307047318_0006 completed successfully
    19/04/26 22:24:19 INFO mapreduce.Job: Counters: 32
    	File System Counters
    		FILE: Number of bytes read=0
    		FILE: Number of bytes written=278672
    		FILE: Number of read operations=0
    		FILE: Number of large read operations=0
    		FILE: Number of write operations=0
    		HDFS: Number of bytes read=76874501
    		HDFS: Number of bytes written=97842427
    		HDFS: Number of read operations=10
    		HDFS: Number of large read operations=0
    		HDFS: Number of write operations=4
    	Job Counters 
    		Launched map tasks=2
    		Data-local map tasks=2
    		Total time spent by all maps in occupied slots (ms)=67897
    		Total time spent by all reduces in occupied slots (ms)=0
    		Total time spent by all map tasks (ms)=67897
    		Total vcore-milliseconds taken by all map tasks=67897
    		Total megabyte-milliseconds taken by all map tasks=69526528
    	Map-Reduce Framework
    		Map input records=4100
    		Map output records=11937375
    		Input split bytes=228
    		Spilled Records=0
    		Failed Shuffles=0
    		Merged Map outputs=0
    		GC time elapsed (ms)=911
    		CPU time spent (ms)=130940
    		Physical memory (bytes) snapshot=373612544
    		Virtual memory (bytes) snapshot=4031164416
    		Total committed heap usage (bytes)=301989888
    	File Input Format Counters 
    		Bytes Read=76874273
    	File Output Format Counters 
    		Bytes Written=97842427
    	wiki
    		stop_words=4588771
    		total_words=11937375
    19/04/26 22:24:19 INFO streaming.StreamJob: Output directory: coursera_mr_task21556317409442921

