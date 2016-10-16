/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/*object Bigrams {

    def main(args: Array[String]) {
        val text = "bible+shakes.nopunc" // input file
	val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
        val input = sc.textFile(text, 2).cache()
	//use sliding(2) to keep pair of words, reduceByKey to sum them up and finally filter out non-bigrams (size<2)
	val counts = input.flatMap(line => line.split(" ").filterNot(x => x=="" || x==" ").sliding(2)).map(word => 	(word.toVector, 1)).reduceByKey((a, b) => a + b).filter{case(vect, freq) => vect.size==2}
	counts.saveAsTextFile("bg")
	//val counts2 = counts.map{case(vect, count) => (vect(1), (vect, count))}
	//counts2.saveAsTextFile("2")
	val counts1 = counts.map{case(vect, count) => (vect(0), (vect, count))}
	//counts1.saveAsTextFile("1")
	//val unionset = counts1.union(counts2)
	//unionset.saveAsTextFile("unionset")
	//group the union by the key (word), sort them as per count in descending order (note the negative sign) and take top 5 bigrams
	val ans = counts1.groupByKey().map{case(key, numbers) => key->numbers.toList.sortBy(-_._2).take(3)}
	//flatten the top 5 values obtained in list format for ease in reading
	val ans1 = ans.flatMap({case (key, numbers) => numbers.map(key-> _)})
	ans1.saveAsTextFile("bg_wc")
	
    }
}*/

object Bigrams {

    def main(args: Array[String]) {
	val text = "bible+shakes.nopunc" // input file
	val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
        val input = sc.textFile(text, 2).cache()
	//use sliding(2) to keep pair of words, reduceByKey to sum them up and finally filter out non-bigrams (size<2)
	val counts = input.flatMap(line => line.split(" ").filterNot(x => x=="" || x==" ").sliding(2)).map(word => 	(word.toVector, 1)).reduceByKey((a, b) => a + b).filter{case(vect, freq) => vect.size==2}
	//counts.saveAsTextFile("bg")
	//val counts2 = counts.map{case(vect, count) => (vect(1), (vect, count))}
	//counts2.saveAsTextFile("2")
	val counts1 = counts.map{case(vect, count) => (vect(0), (vect, count))}
	//counts1.saveAsTextFile("1")
	//val unionset = counts1.union(counts2)
	//unionset.saveAsTextFile("unionset")
	//group the union by the key (word), sort them as per count in descending order (note the negative sign) and take top 5 bigrams
	val ans = counts1.groupByKey().map{case(key, numbers) => key->numbers.toList.sortBy(-_._2).take(3)}
	//flatten the top 5 values obtained in list format for ease in reading
	val ans1 = ans.flatMap({case (key, numbers) => numbers.map(key-> _)})
        //val bg = "bg.txt"
	//val text = "bg_wc.txt"
	//val conf = new SparkConf().setAppName("Simple Application")
        //val sc = new SparkContext(conf)
        //val wc = sc.textFile(text, 2).cache()
	//val wc1 = wc.flatMap(line => line.split(" "))
	var wordcount = 0;
	var paircount = 0;
	var input1 = Console.readLine("First word?")
	while(input1!=":stop")
	{
		
	ans1.filter{case(a, vector) => a==input1}.foreach{case(a, (vector,b)) => println(vector(1))}
	input1 = Console.readLine("Next?")	// input is the key. In wc, find all keys "input" and output vect(1) for each key in order
		// user enters - 1/2/3 or a different word.
		// If user input = 1/2/3 -> input = vect(1)(i) for i=1/2/3
		// If user inputs a different word, input = "new word"
		// Keep iterting till user enters ":stop:"		
	}

	
    }
}


