from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Word count")
sc = SparkContext(conf=conf)

book = sc.textFile("../data/Book.txt")
# Split every word
words = book.flatMap(lambda x: x.split())

# Get count for every word
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    # Get sure that encoding is right and can display in console
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord, count)
