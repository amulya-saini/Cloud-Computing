from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create Spark Context with two working threads (note, `local[5]`)
sc = SparkContext("local[2]", "NetworkWordFilterCount")

# Create local StreamingContextwith batch interval of 5 second
ssc = StreamingContext(sc, 5)

# Create DStream that will connect to the stream of input lines from connection to localhost:9991
lines = ssc.socketTextStream("localhost", 9991)

# Split lines into words
words = lines.flatMap(lambda line: line.split(" "))

# Filter words based on criteria
filtered_words = words.filter(lambda word: word.startswith('st') and ('a' in word or word.endswith('r')))

# Map words to pairs for counting
pairs = filtered_words.map(lambda word: (word, 1))

# Count each word in each batch
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Filter for words with a frequency between 2 and 10
filtered_wordCounts = wordCounts.filter(lambda x: 2 < x[1] < 10)

# Print the first ten elements of each RDD generated in this DStream to the console
filtered_wordCounts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()
