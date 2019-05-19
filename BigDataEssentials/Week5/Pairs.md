

```python
from __future__ import division
from pyspark import SparkConf, SparkContext
import re

try:
    sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local").set("spark.cores.max", "4"))
except:
    pass


def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []

def pairs(words, first_word='word'):
    pairs = []
    
    for i, word in enumerate(words[:-1]):
        if (word == first_word):
            pair = '{}_{}'.format(word, words[i+1])
            cnt = 1
            pairs.append((pair, cnt))
        else:
            continue
    return pairs

wiki = sc.textFile("/data/wiki/en_articles_part/articles-part", 4).map(parse_article)

# lowercase all words
wiki_lower = wiki.map(lambda words: [x.lower() for x in words])

# find pairs starting from defined word
wiki_pairs = wiki_lower.flatMap(lambda x: pairs(x, 'narodnaya'))

# filtering empty elements
wiki_pairs = wiki_pairs.filter(lambda x: x != [])

# aggregate counters
wiki_red = wiki_pairs.reduceByKey(lambda a, b: a + b, numPartitions=16)

# sort values by key
wiki_red_sorted = wiki_red.sortByKey()


result = wiki_red_sorted.collect()
for pair, cnt in result:
    print '{}\t{}'.format(pair, cnt)

```

    narodnaya_gazeta	1
    narodnaya_volya	9

