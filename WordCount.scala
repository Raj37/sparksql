import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount{
	def main(args:Array[String]){
	val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
	val sc = new SparkContext(conf)
	val file = sc.textFile("file.txt")
	val count = file.flatMap(line=>line.split("")).map(rec=>(rec,1)).reduceByKey((x,y)=>x+y)
	print(count)
 }
}
