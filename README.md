# Cloud-Computing
All the work done in relation to the Cloud computing course.

Assignment_1:
write a Python program that will index a set of documents and build a function to search through these documents using the index.

Assignment_2: 
Complete the following using spark:

Read the data (all the files in the ‘data’ directory) using the function textFile.
Take only the “text” part of each article and count the frequency of all the words (convert the text into lowercase).
Remove (Filter) any word whose frequency is less than 10.

Report the following:
- Total size of the output data (after the filtering). 
- Frequency of the following words – congress, London, Washington, football.
- The word with maximum frequency for each month (hint: to read only a month’s articles you can use ‘*’. E.g. for February ‘2012-02*’ represents all files starting with 2012-02,i.e. files belonging to Feb).
- The list of words appeared on ‘2012-09-01’ but not on ‘2012-08-01’.
- Frequency of the word ‘monsoon’ for all months.

Assignment_3:
- Using Spark MLlib build a model to predict taxi fare from trip distance.
- Using Spark MLlib build a model to predict taxi fare from trip distance and trip duration in minutes.
* What is the fare of a 20-mile-long trip using M1
* What is the fare of a 14-mile trip that took 75 minutes using M2
* Which fare is higher 10 mile trip taking 40 min or 13 mile trip taking 25 min?
- Using Spark operations (transformation and actions) compute the average tip amount
- During which hour the city experiences the most number of trips? E.g. 10 am-11 am or 4 pm-5 pm
- Compare Spark’s performance
* Divide the data into 10 parts: 10%, 20%, …, 100%
* Plot the time taken by each method and save it in PNG format (or display the plot if you are using Jupyter Notebook or Google Colab).

  
Lab_2:
Compute incoming edges using spark functions

Show the following

1. Python list: Output sorted (based on the node id) list of incoming edges

2. Python dictionary: Show only the nodes who has exactly 5 incoming edges

3. Python list: show the incoming edges of the node ‘1’

Lab_3:

Lab_4:
Write a program that will listen to an input continuous text stream and output words if:

1. the word starts 'st'

2. the word contains 'a' or endswith 'r'

3. the frequency is greater 2 and less than 10
