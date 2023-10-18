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
