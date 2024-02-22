from pyspark import SparkConf, SparkContext
from operator import add
import re

conf = SparkConf().setMaster("local").setAppName("Word count")
sc = SparkContext(conf=conf)


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


book = sc.textFile("../data/Book.txt")
# Split every word
words = book.flatMap(normalizeWords)

# Get count for every word
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    # Get sure that encoding is right and can display in console
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord, count)

# Get rdd with each word and 1
wordsWithCount = words.map(lambda word: (word, 1)).reduceByKey(add)
# Swap value with key, and sort by Key(old Value)
sortedWordCounts = wordsWithCount.map(lambda word: (word[1], word[0])).sortByKey()
# Swap value with key to get Tuple(word, count)
sortedWordCounts = sortedWordCounts.map(lambda wordCount: (wordCount[1], wordCount[0]))

for word, count in sortedWordCounts.collect():
    # Get sure that encoding is right and can display in console
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord, count)
