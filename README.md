#Bigram Prediction using Maximum Likelihood Technique

What does the program do?

Given the nth word, three possibilities of the the (n+1)th word are evaluated and displayed to the user. The user selects a word
among the three options or enters a different word and the process continues until the user enters ":stop" (without quotes) as input.

How does the program calculate top 3 possibilities?

The text dataset used is "bible+shakes.nopunc". Firstly, all bigrams (pairs of 2 words) appearing in the text are found and their
frequencies calculated. For each word in the text, we filter out top 3 bigrams (on count basis) in which the word appears as the first word.
Note that this is same as the maximum likelihood technique. 

P[W(n)|W(n-1)] = P[W(n)∩W(n-1)]/P[W(n-1)]

Since we filter out all bigrams on the basis of first word, the denominator is same for all candidates and only the frequency decides
the next 3 candidates.

How to run the code?

1. Set up your SPARK_HOME.
2. Make sure you are in the repository folder while on the terminal.
3. Assuming sbt is already installed, run 'sbt compile' and 'sbt package' on the terminal.
4. Now that the jar file is ready and placed at 'target/scala-2.11/simple-project_2.11-1.0.jar', run the following command:
   $SPARK_HOME/bin/spark-submit --class "SimpleApp" --master "local[4]" target/scala-2.11/simple-project_2.11-1.0.jar from the
   terminal.
5. Input the words you want to enter and see the hints appearing.
6. Use ":stop" (without quotes) as input to stop the code.

For any queries/suggestions please write to sodhanipranav[at]cs.ucla.edu.
