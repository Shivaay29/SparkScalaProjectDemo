# Project 3
**(Due Oct 27, midnight, *no late submissions*)**

Project 5 focuses on using Apache Spark for doing large-scale data analysis tasks. For this assignment, we will use relatively small datasets and  we won't run anything in distributed mode; however Spark can be easily used to run the same programs on much larger datasets.

## Getting Started with Spark

This guide is basically a summary of the excellent tutorials that can be found at the [Spark website](http://spark.apache.org).

[Apache Spark](https://spark.apache.org) is a relatively new cluster computing framework, developed originally at UC Berkeley. It significantly generalizes
the 2-stage Map-Reduce paradigm (originally proposed by Google and popularized by open-source Hadoop system); Spark is instead based on the abstraction of **resilient distributed datasets (RDDs)**. An RDD is basically a distributed collection 
of items, that can be created in a variety of ways. Spark provides a set of operations to transform one or more RDDs into an output RDD, and analysis tasks are written as
chains of these operations.

Spark can be used with the Hadoop ecosystem, including the HDFS file system and the YARN resource manager. 

### Installing Project3

Download the repository as `git clone https://gitlab.cs.umd.edu/keleher/p3`. 

We are ready to use Spark. 

### Spark and Python

Spark primarily supports three languages: Scala (Spark is written in Scala), Java, and Python. We will use Python here -- you can follow the instructions at the tutorial
and quick start (http://spark.apache.org/docs/latest/quick-start.html) for other languages. The Java equivalent code can be very verbose and hard to follow. The below
shows a way to use the Python interface through the standard Python shell.

### Jupyter Notebook

To use Spark within the Jupyter Notebook (and to play with the Notebook we have provided), you can do (from the `/vagrant` directory on the VM):
	```
	jup
	```
### PySpark Shell

You can also use the PySpark Shell directly.

1. `$SPARKHOME/bin/pyspark`: This will start a Python shell (it will also output a bunch of stuff about what Spark is doing). The relevant variables are initialized in this python
shell, but otherwise it is just a standard Python shell.

2. `>>> textfile = sc.textFile("README.md")`: This creates a new RDD, called `textfile`, by reading data from a local file. The `sc.textFile` commands create an RDD
containing one entry per line in the file.

3. You can see some information about the RDD by doing `textfile.count()` or `textfile.first()`, or `textfile.take(5)` (which prints an array containing 5 items from the RDD).

4. We recommend you follow the rest of the commands in the quick start guide (http://spark.apache.org/docs/latest/quick-start.html). Here we will simply do the Word Count
application.

#### Word Count Application

The following command (in the pyspark shell) does a word count, i.e., it counts the number of times each word appears in the file `README.md`. Use `counts.take(5)` to see the output.

`>>> counts = textfile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)`

Here is the same code without the use of `lambda` functions.

```
def split(line): 
    return line.split(" ")

def generateone(word): 
    return (word, 1)

def sum(a, b):
    return a + b

count = textfile.flatMap(split).map(generateone).reduceByKey(sum)
```

The `flatmap` splits each line into words, and the following `map` and `reduce` do the counting (we will discuss this in the class, but here is an excellent and detailed
description: [Hadoop Map-Reduce Tutorial](http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html#Source+Code) (look for Walk-Through).

The `lambda` representation is more compact and preferable, especially for small functions, but for large functions, it is better to separate out the definitions.

### Running it as an Application

Instead of using a shell, you can also write your code as a python file, and *submit* that to the spark cluster. The `project5` directory contains a python file `wordcount.py`,
which runs the program in a local mode. To run the program, do:
`$SPARKHOME/bin/spark-submit wordcount.py`

### More...

We encourage you to look at the [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html) and play with the other RDD manipulation commands. 
You should also try out the Scala and Java interfaces.

## Assignment Details

We have provided a Python file: `assignment.py`, that initializes the folllowing RDDs:
* An RDD consisting of lines from a Shakespeare play (`play.txt`)
* An RDD consisting of lines from a log file (`NASA_logs_sample.txt`)
* An RDD consisting of 2-tuples indicating user-product ratings from Amazon Dataset (`amazon-ratings.txt`)
* An RDD consisting of JSON documents pertaining to all the Noble Laureates over last few years (`prize.json`)

The file also contains some examples of operations on these RDDs. 

Your tasks are to fill out the 8 functions that are defined in the `functions.py` file (starting with `task`). The amount of code that you 
write would typically be small (several would be one-liners), with the exception of the last one. 

- **Task 1 (4pt)**: This takes as input the playRDD and for each line, finds the first word in the line, and also counts the number of words. It should then filter the RDD by only selecting the lines where no stop words are defined (the list stop words to be considered is given). The output will be an RDD where the key is the first word in the line, and the value is a 2-tuple, the first being the line and the second being the number of words. Simplest way to do it is probably a `map` followed by a `filter`.

```
stop_words = ['is','am','are','the','for','a','of','in']
```

- **Task 2 (4pt)**: Write just the flatmap function (`task2_flatmap`) that takes in a parsed JSON document (from `prize.json`) and returns the surnames of the Nobel Laureates. In other words, the following command should create an RDD with all the surnames (the distinct() is to remove duplicated empty surnames). We will use `json.loads` to parse the JSONs (this is already done). Make sure to look at what it returns so you know how to access the information inside the parsed JSONs (these are basically nested dictionaries). (https://docs.python.org/2/library/json.html)
```
     	task2_result = nobelRDD.map(json.loads).flatMap(task2_flatmap).distinct()
```

- **Task 3 (4pt)**: Write with the flatmap function that takes in a parsed JSON document (from prize.json) and returns the surnames of the Nobel Laureates in each year from 2004 to 2016. It should create an RDD the key is the years, and the value is a list of all the surnames of the Nobel Laureates in the year.
We will use json.loads to parse the JSONs (this is already done). Make sure to look at what it returns so you know how to access the information inside the parsed JSONs (these are basically nested dictionaries). (https://docs.python.org/2/library/json.html)

- **Task 4 (4pt)**: This function operates on the logsRDD. It takes as input a list of logs and returns an RDD where the key is the hosts and the value is the latest dates and time (no time zone) in the log when the hosts are visited.
The format of the log entries should be self-explanatory, but here are more details if you need: [NASA Logs](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html).
 
- **Task 5 (4pt)**: Complete a function to group all ratings(or users) of products and calculate the degree distribution of product nodes in the Amazon graph. In other words, calculate the degree of each product node (i.e., number of ratings each product has gotten). Use a groupByKey to find the list of ratings(or users) each product has got and reduceByKey (or aggregateByKey) to find the degree of each product rating. The output should be a RDD where the key is the product, and the values are the degree and a list of all ratings(or users) the product has gotten. Make sure to make all the ratings a list and join the two RDDs.

Note: In this question, both list of ratings and users can be accepted as the values in the answer RDD(the result.txt is taking users as the value). We highly recommend you to implement the solution with list of users, since the input of Task5 function is a tuple of (user, product), which means you don't need to make any change to assignment.py
 
- **Task 6 (4pt)**: On the logsRDD, for two given times, use a 'cogroup'(function in Spark) to create the following RDD: the key of the RDD will be a host, and the value will be a 2-tuple, where the first element is a list of all URLs fetched from that host before the first time, and the second element is the list of all URLs fetched from that host after the second time. Use filter to first create two RDDs from the input logsRDD.

- **Task 7 (8pt)**: Your task is to write a name counting application for counting the first names of the Nobel Prizes for each category. The return value should be a PairRDD where the key is a string, of which the format is "[Category]:[Firstname]" (i.e., "chemistry:Michael"), and the value is its count, i.e., in how many times did that combination appear.

- **Task 8 (8pt)**: [Maximal Matching] `task8` should implement one iteration of a greedy algorithm for finding a maximal matching in a bipartite graph. 
A *matching* in a graph is a subset of the edges such that no two edges share a vertex (i.e., every vertex is part of at most 1 edge in the matching). A *maximal* matching
is such that, we cannot add any more edges to it (in other words, there is no remaining edge in the graph both of whose endpoints are unmatched). Here is a simple greedy
algorithm for finding a maximal matching using map-reduce; note that this is not a particularly good algorithm for solving this problem, but it is easy to parallelize.
We maintain the current state of the program in a PairRDD called currentMatching, where we note all the user-product relationships
that have already been chosen. Initially this RDD is set to be empty (for making it easy to debug, we have added one entry to it).
The following is then executed repeatadly till currentMatching does not change.
  * For each user who is currently unmatched (i.e., does not have an entry in currentMatching), find the group of products connected to it that are also unmatched.
  * For each such user, among the group of unmatched products it is connected to, pick the `min` (it is better to pick this randomly but then the output is not deterministic and will make testing/debugging difficult)
  * It is possible that we have picked the same product two different unmatched users, which would violate the matching constraint.
  * In another step, repeat the same process from the products' perspective, i.e., for each product that has been picked as potential match for multiple user nodes, pick the minimum user node (again doing this randomly is better).
  * Now we are left with a set of user-product relationships that we can add to currentMatching and iterate

### Sample results.txt File
You can use spark-submit to run the `assignment.py` file, but it would be easier to develop with pyspark (by copying the commands over). We will also shortly post iPython instructions.

**results.txt** shows the results of running assignment.py on our code using: `$SPARKHOME/bin/spark-submit assignment.py`

### Submission

Submit the `functions.py` file <a href="https://umd.instructure.com/courses/1267269/assignments/4983016">here</a>.

