/* Bigrams.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Bigrams {

    def main(args: Array[String]) {
	val text = "bible+shakes.nopunc" // input file
	val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
        val input = sc.textFile(text, 2).cache()
	//use sliding(2) to keep pair of words, reduceByKey to sum them up and finally filter out non-bigrams (size<2)
	val counts = input.flatMap(line => line.split(" ").filterNot(x => x=="" || x==" ").sliding(2)).map(word => 	(word.toVector, 1)).reduceByKey((a, b) => a + b).filter{case(vect, freq) => vect.size==2}
	val counts1 = counts.map{case(vect, count) => (vect(0), (vect, count))}
	val ans = counts1.groupByKey().map{case(key, numbers) => key->numbers.toList.sortBy(-_._2).take(3)}
	//flatten the top 3 values obtained in list format for ease in access
	val ans1 = ans.flatMap({case (key, numbers) => numbers.map(key-> _)})
        var input1 = Console.readLine("Let's begin ... \n")
	var total: String = input1+" ";
	while(input1!=":stop")
	{
	var op = ans1.filter{case(a, vector) => a==input1}.map{case(a, (vector, b)) => vector(1)}.foreach{ln => println(ln)}
	input1 = Console.readLine(total)
	if(input1!=":stop") {total = total + input1 + " ";	}
	}
    }
}


