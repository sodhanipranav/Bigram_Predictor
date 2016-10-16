## Spark 

[Apache Spark](https://spark.apache.org) is a relatively new cluster computing framework, developed originally at UC Berkeley. It significantly generalizes
the 2-stage Map-Reduce paradigm popularized by Google and Apache Hadoop, and is instead based on the abstraction of **resilient distributed datasets (RDDs)**. An RDD is basically a distributed collection 
of items, that can be created in a variety of ways. Spark provides a set of operations to transform one or more RDDs into an output RDD, and analysis tasks are written as
chains of these operations.

Spark can be used with the Hadoop ecosystem, including the HDFS file system and the YARN resource manager. 

### Installing Spark

Complete instructions found [HERE](https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm)

Quick Install Instructions are as follows.

1. Download the Spark package at http://spark.apache.org/downloads.html. Use the latest version.

2. Move the extract file to the lab6 directory in the git repository, and uncompress it using: 

`tar zxvf spark-*.tgz`

3. This will create a new directory: `spark-XXX` -- `cd` into that directory.

We are ready to use Spark. 

### Using Spark

Spark provides support for three languages: Scala (Spark is written in Scala), Java, and Python. We will use Scala here -- you can follow the instructions at the tutorial
and quick start (http://spark.apache.org/docs/latest/quick-start.html) for other languages. 

Scala is a JVM-based functional programming language, but it's syntax and functionality is quite different from Java. 
The [Wikipedia Article](http://en.wikipedia.org/wiki/Scala_%28programming_language%29) is a good start to learn about Scala, 
and there are also quite a few tutorials out there. For this assignment, we will try to minimize the amount of Scala you have
to learn and try to provide sufficient guidance.


1. `./bin/spark-shell`: This will start a Scala shell (it will also output a bunch of stuff about what Spark is doing). The relevant variables are initialized in this shell, but otherwise it is just a standard Scala shell.

2. `> val textFile = sc.textFile("README.md")`: This creates a new RDD, called `textFile`, by reading data from a local file. The `sc.textFile` commands create an RDD
containing one entry per line in the file.

3. You can see some information about the RDD by doing `textFile.count()` or `textFile.first()`, or `textFile.take(5)` (which prints an array containing 5 items from the
        RDD).

4. We recommend you follow the rest of the commands in the quick start guide (http://spark.apache.org/docs/latest/quick-start.html). Here we will simply do the Word Count
application.

### Word Count Application

`val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b)  => a + b)`

The `flatmap` splits each line into words, and the following `map` and `reduce` basically do the word count (in a similar fashion to standard MapReduce wordcount -- see, e.g., [link](http://kickstarthadoop.blogspot.com/2011/04/word-count-hadoop-map-reduce-example.html).

### Running it as an Application

Instead of using a shell, you can also write your code as a Scala file, and *submit* that to the spark cluster. The Assignment2 directory contains the appropriate files (`build.sbt` and `src/main/scala/SimpleApp.scala`) for doing this. First you need to assemble a jar file using `sbt package` command. 
This creates a jar file containing the app.

Then the following command executes the Spark job in a local manner (a simple change to the command can do this on a cluster, assuming you have
the cluster already running).

`$SPARK_HOME/bin/spark-submit --class "SimpleApp" --master "local[4]"   target/scala-2.11/simple-project_2.11-1.0.jar`

### More...

We encourage you to look at the [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html) and play with the other RDD manipulation commands. 

### Assignment Part 1

- [Bigrams](http://en.wikipedia.org/wiki/Bigram) are simply sequences of two consecutive words. For example, the previous sentence contains the following bigrams: "Bigrams
are", "are simply", "simply sequences", "sequences of", etc.
- Modify SimpleApp.scala to write a **Bigram Counting** application that can be composed as two MapReduce jobs. 
    - The first MapReduce job counts bigrams.
    - The second MapReduce job takes the output of the first job (bigram counts) and computes for each word the top 5 bigrams by count that it is a part of, and the bigram count associated with each.
- You can use the sample collection input file "bible+shakes.nopunc.gz" to test your programs.

### Assignment Part 1 Submission

Submit the following files: (1) Your Scala file, and (2) The output file, using the provided `submission.txt`.

---

## Spark Streaming

[Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html) is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Spark Streaming can ingest data from  many sources like Kafka, Flume, Twitter, ZeroMQ, Kinesis or plain old TCP sockets and can process the stream using complex algorithms expressed with high-level functions like map, reduce, join and window.

We will use Spark Streaming Java API for this assignment. 

#### Using Spark Streaming

Running a Netcat Server:	
- Open a new terminal and type `nc -lk 9999`
- This starts Netcat Server bound to port 9999. You can now type on this terminal to send data to the Netcat server
- Any client listening to port 9999 will receive anything thats typed on the Netcat Server terminal 


Running the provided Spark application:
- `cd streaming`
- `mvn package`: This will compile and create a `jar` file (in target/ directory). (**You may need to install `maven` for this purpose.**)
- `YOUR_SPARK_HOME/bin/spark-submit --class JavaNetworkWordCount --master 'local[4]' target/streaming-project-1.0.jar`
      - Make sure to replace YOUR_SPARK_HOME with the appropriate directory (where you downloaded Spark)

Note: The `Spark-Assignment/log4j.properties` suppresses all the extraneous output, so the first output you will see will be after 10 seconds.

### What does the program do?
		

The main file here is: `JavaNetworkWordCount.java` underneath `src`
- It starts listerning to port 9999 (where the Netcat server is sending data)
- It reads each line from the Netcat server, splits them by space, produces a tuple (word, 1) for each word, and then counts them
- Note that the program sets a *batch size* of 10 seconds, which means that the line "wordCounts.print();" will be executed every 10 seconds. *Batch size* is a key notion in Spark Streaming. Spark streaming processes all the data tuples it has received at the end of each batch interval.
- Anything you *type on the Netcat server terminal* Spark Streaming will process that. Spark Streaming will print an empty line (with ending time of the current batch) if nothing is typed on the Netcat terminal in a batch window of 10 seconds. 
  
---

### Assignment Part 2

For this assignment we will learn to implement sliding window using Spark streaming. To be specific, the task is to count the number of times '#Obama' appeared in the Netcat
server between last 30 seconds and current time (i.e., 30s of *window interval*). Moreover, we want to do this computation every 10 seconds (i.e., 10s of *sliding
        parameter*). Note that these two concepts are different from the *batch size* which is Spark specific. For more details read the [Window
Operation](https://spark.apache.org/docs/latest/streaming-programming-guide.html) section. And example input to the Netcat server could be found in `NetcatInputExample.txt`. You can simply copy the text from the file and paste on the Netcat server terminal multiple times with varying time interval.   

You would need to make changes in the following file: `Assignment.java`

You can run it using the same command as before with replacing JavaNetworkWordCount with Application, i.e., :
     `YOUR_SPARK_HOME/bin/spark-submit --class Assignment --master 'local[4]' target/streaming-project-1.0.jar`

### Assignment Part 2 Submission
Submit your Assignment.java file, and the output of one execution run (you don't need to submit the netcat input) using the provided `submission.txt`.

---

## Spark SQL

In this assignment, you'll look at data from the Federal Election Commission
(FEC), which contains details on the campaign finances of the current
candidates for the presidency of the United States.

You will use Spark DataFrames to run some basic analytical queries on your data. You'll see that Spark
is able to behave like a traditional database management system, and its API of query operators easily supports queries handled by other SQL systems in a scalable fashion.

## The Data: Federal Election Commission Finance Data

You will be performing data analytics on about 432 MB of FEC data.
This data describes the finances of candidates running for election in 2016, and
it details contributions from individual and organizations to campaigns
disclosed to the FEC.

This data is stored [here]
(https://1drv.ms/u/s!AvL8BEvGdNK0hYU-sWKejLzbtBdTvw).
Each table consists of a header `.csv` file and a data `.txt` file. For example,
this is the Committee Master header file (`cm_header_file.csv`)

```
MTE_ID,CMTE_NM,TRES_NM,CMTE_ST1,CMTE_ST2,CMTE_CITY,CMTE_ST,CMTE_ZIP,CMTE_DSGN,CMTE_TP,CMTE_PTY_AFFILIATION,CMTE_FILING_FREQ,ORG_TP,CONNECTED_ORG_NM,CAND_ID
```

and this is the first four rows of the corresponding data file (`cm.txt`)

```
C00000059|HALLMARK CARDS PAC|BROWER, ERIN MS.|2501 MCGEE|MD#288|KANSAS CITY|MO|64108|U|Q|UNK|M|C|HALLMARK CARDS, INC.|
C00000422|AMERICAN MEDICAL ASSOCIATION POLITICAL ACTION COMMITTEE|WALKER, KEVIN|25 MASSACHUSETTS AVE, NW|SUITE 600|WASHINGTON|DC|20001|B|Q||M|M|AMERICAN MEDICAL ASSOCIATION|
C00000489|D R I V E POLITICAL FUND CHAPTER 886|TOM RITTER|3528 W RENO||OKLAHOMA CITY|OK|73107|U|N||Q|L|TEAMSTERS LOCAL UNION 886|
C00000547|KANSAS MEDICAL SOCIETY POLITICAL ACTION COMMITTEE|C. RICHARD BONEBRAKE, M.D.|623 SW 10TH AVE||TOPEKA|KS|66612|U|Q|UNK|Q|T||
```

Here's a quick summary of the tables:
* [**cm**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteeMaster.shtml)
contains committee information
* [**cn**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandidateMaster.shtml)
contains candidate information
* [**ccl**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandCmteLinkage.shtml)
contains linkage between committees and candidates
* [**itcont**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml)
contains individual contributions to committees
* [**itpas2**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionstoCandidates.shtml)
contains contributions between committees
* [**itoth**](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteetoCommittee.shtml)
links between committees

Read the specification at the
[FEC site](http://www.fec.gov/finance/disclosure/ftpdet.shtml#a2015_2016)
to further familiarize yourself with the details of these formats.

## The System: Spark SQL (DataFrames)

You'll be using [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview)
to process the FEC data. Essentially, Spark SQL is an extension to Spark which
supports and exploits database abstractions to allow applications to manipulate
data both in the context of SQL and in the context of Spark operators.

Spark DataFrames are a specialization of the RDD which additionally organize RDD
records into columns. In fact you can always access the underlying RDD for a
given dataframe using `dataframe.rdd`.  Each DataFrame represents a SQL table,
which support as methods logical SQL operators composable with other Spark and
user-defined functions.

For instance, suppose we have a SQL table containing two-dimensional points,

```sql
CREATE TABLE points (x int, y int)
```

and we want to sample 20% of the x-values in the fourth quadrant into a
table `Samples`. In SQL we could write

```sql
CREATE TABLE samples AS
SELECT x
FROM points
WHERE x > 0
 AND y < 0
 AND rand() < 0.2;
```

Alternatively, we can read this table into a Spark DataFrame, `points`

```python
points = some_loading_function(points_data)
points.registerTempTable("points")
```

We then can compute, save, and print this sampling of points

```python
samples = points.where(points.x > 0)\
    .where(points.y < 0)\
    .sample(False, 0.2)\
    .select('x')\
    .registerTempTable("samples")

samples.show()
```

In fact, due to this duality, DataFrames support direct SQL queries as
well. To run the above query on a DataFrame, we write

```python
sql.sql("SELECT x FROM points WHERE x > 0 AND y < 0 AND rand() > 0.2")\
    .registerTempTable("samples")
```

Play around with DataFrames to get a sense of how they work. You can read the
[official documentation](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview) for more details.

### Assignment Part 3

Time to put your distributed database skills to use!

## 1. DataFrames

Your first task is to carry out basic data analytics queries against the FEC data.

### Basic Analytics Queries

Now that our files have been loaded into Spark, we can use DataFrames to perform
some basic analytical queries - similar to what we can do with SQL. Observe that
we can either write raw SQL queries, or we can use DataFrame methods to directly
apply SQL operators. For this question, you may use either approach.

Answer the following questions by writing queries:
 1. What are the ID numbers and Principal Candidate Committee numbers of the 4 current
    presidential candidate front-runners (Hillary Clinton, Bernie Sanders, Donald Trump,
    and Ted Cruz)?
    Hint: Take a look at the output of the demonstration. What values do we want
    these columns to have?
 2. How many contributions by individuals has each front-runner's principal campaign committee received? 
    Hint: Which table might you want to join on? Do _not_ filter by ENTITY_TP.
 3. How much in total has each front-runner's principal campaign committee received from the contributions in Q2?
 4. What are the committees that are linked to each of the front-runners?
    Hint: How many tables will we need for this?
 5. How many contributions by committees has each front-runner received? 
    Hint: Do _not_ filter by ENTITY_TP.
 6. How much in total has each front-runner received from the contributions in Q5?

**Note: The penultimate line of each cell describes the schema for the output
we're expecting. If you change this line, make sure that your output's schema
matches these column names.**

### Assignment Part 3 Submission
Add your final queries to submission.txt. The queries can be specified in direct SQL or using the DataFrame API.


## GraphX (OPTIONAL)

This part is optional, but you can do it if you want to learn the Graph Analytics functionality offered by Spark.

GraphX is a graph analytics platform based on Apache Spark. GraphX does not have a Python API, so you will have to program in Scala.

Following are brief step-by-step instructions to get started with GraphX. The [GraphX Getting Started
Guide](http://spark.apache.org/docs/latest/graphx-programming-guide.html#getting-started) goes into much more depth about the data model, and the functionalities.
The following examples are taken either from that guide, or from [another guide](https://github.com/amplab/datascience-sp14/blob/master/lab10/graphx-lab.md) by the authors.

We will use the *Spark Scala Shell* directly. It might be better for you to write your code in a text editor and cut-n-paste it into the shell.


* Start the Spark shell. This is basically a Scala shell with appropriate libraries loaded for Spark, so you can also run Scala commands here directly. Here `SPARK_HOME`
denotes the directory where you have extracted Spark (for previous assignments).
```
SPARK_HOME/bin/spark-shell
```

* Import the GraphX Packages. We are ready to start using GraphX at this point.
```
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
```

* Load some data. First we will define two arrays.
```
val vertexArray = Array(
  (1L, ("Alice", 28)),
  (2L, ("Bob", 27)),
  (3L, ("Charlie", 65)),
  (4L, ("David", 42)),
  (5L, ("Ed", 55)),
  (6L, ("Fran", 50))
  )
val edgeArray = Array(
  Edge(2L, 1L, 7),
  Edge(2L, 4L, 2),
  Edge(3L, 2L, 4),
  Edge(3L, 6L, 3),
  Edge(4L, 1L, 1),
  Edge(5L, 2L, 2),
  Edge(5L, 3L, 8),
  Edge(5L, 6L, 3)
  )
```

* Then we will create the graph out of them, by first creating two RDDs. The first two statements create RDDs by using the `sc.parallelize()` command.
```
val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
```

* The Graph class supports quite a few operators, most of which return an RDD as the type.
    - `graph.vertices.collect()`: `graph.vertices` just returns the first RDD that was created above, and `collect()` will get all the data from the RDD and print it (this should only be done for small RDDs)
    - `graph.degrees`: This returns an RDD with the degree for each vertex -- use `collect()` to print and see
See the Getting Started guide for other built-in functions.


* The following code finds the users who are at least 30 years old using `filter`.
```
graph.vertices.filter { case (id, (name, age)) => age > 30 }.foreach { case (id, (name, age)) => println(name + " is " + age) }
```

`case` is a powerful construct in Scala that is used to do pattern matching.

* Graph Triplets: One of the core functionalities of GraphX is exposed through the RDD `triplets`. There is one triplet for each edge, that contains information about
both the vertices and the edge information. Take a look through:
`graph.triplets.collect()`

The output is somewhat hard to parse, but you can see the first entry is: `((2,(Bob,27)),(1,(Alice,28)),7)`, i.e., it contains the full information for both the endpoint
vertices, and the edge information itself. 

More specifically, a triplet has the following fields: `srcAttr` (the source vertex property), `dstAttr`, `attr` (the edge property), `srcID` (source vertex Id), `dstId`

The following commands will print out information for each edge using the triplets. Note that the following would be hard to do without using triplets, because the data is
split across multiple RDDs.

`graph.triplets.foreach {t => println("Source attribute: " + t.srcAttr + ", Destination attribute: " + t.dstAttr + ", Edge attribute: " + t.attr)}`

The following command shows another use of `case` to retrieve information from within `srcAttr`. This is the preferred way of doing `casting` in Scala.

`graph.triplets.foreach {t => t.srcAttr match { case (name, age) => println("Source name: " + name)} }`

* The `subgraph` command can be used to create subgraphs by applying predicates to filters. 

`val olderUsers = graph.subgraph(vpred = (id, attr) => attr._2 > 30)`

You can verify that only the vertices with age > 30 are present by doing `g1.vertices.collect()`


* The core aggregation primitive in GraphX is called `mapReduceTriplets`, and has the following signature.

```
class Graph[VD, ED] {
  def mapReduceTriplets[A](
      map: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduce: (A, A) => A)
    : VertexRDD[A]
}
```

`mapReduceTriplets` applies the user-provided `map` operation to each triplet (in `graph.triplets`), resulting in messages being sent to either of the endpoints. The
messages are aggregate using the user-provided `reduce` function.

The following code uses this operator on a randomly generated graph to compute the average age of older followers for each user. The example is copied verbatim from 
the Getting Started guide, where more details are provided.

```
// Import random graph generation library
import org.apache.spark.graphx.util.GraphGenerators
// Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
val graph: Graph[Double, Int] =
  GraphGenerators.rmatGraph(sc, 40, 200).mapVertices( (id, _) => id.toDouble )
// Compute the number of older followers and their total age
val olderFollowers: VertexRDD[(Int, Double)] = graph.mapReduceTriplets[(Int, Double)](
  triplet => { // Map Function
    if (triplet.srcAttr > triplet.dstAttr) {
      // Send message to destination vertex containing counter and age
      Iterator((triplet.dstId, (1, triplet.srcAttr)))
    } else {
      // Don't send a message for this triplet
      Iterator.empty
    }
  },
  // Add counter and age
  (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
)
// Divide total age by number of older followers to get average age of older followers
val avgAgeOfOlderFollowers: VertexRDD[Double] =
  olderFollowers.mapValues( (id, value) => value match { case (count, totalAge) => totalAge / count } )
// Display the results
avgAgeOfOlderFollowers.collect.foreach(println(_))
```

Note the key functions here are the `map` function where for every triplet, a message is generated if the follower is older than the user (i.e., if `srcAttr > dstAttr`). 
The messages are aggregated by summing, so at the end of the `reduce`, we have the total number of older followers as well as a sum of their ages.

The code afterwards simply finds the average age.

### Assignment Part 3

Add both your commands/code and the output (truncated if it is too much) into the `submission.txt` file.

1. Understand and print the output of the `graph.triangleCount` function. The output should look like: "Bob participates in 2 triangles." (with one line per user).

1. Understand and print the output of the `olderUsers.connectedComponents` function. The output should look like: "Bob is in connected component 1" (with one line per user).

1. Modify the `mapReduceTriplets`-based aggregation code above to find, for each user (in the randomly generated graph), the followers with the maximum and second-maximum
ages. 

1. The provided file `states.txt` contains code to generate a small graph where each node is a state, each edge denotes a border between two states and the property of the
edge is the length of the border. Modify the above `mapReduceTriplets`-based aggreagtion code to find, for each state, the state with which it shares the longest border. 
Note that DC is counted is a state here, whereas Alaska and Hawaii are not present in the dataset.
