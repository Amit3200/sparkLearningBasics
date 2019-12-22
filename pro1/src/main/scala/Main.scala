import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object workRead {

  def main(args:Array[String]){
    // dataframe column to array
    // var wordTest=df1.select("word").rdd.map(r=>r.getString(0)).collect.toList
    evaluatePractice1()
    evaluatePractice2()
  }

  def evaluatePractice1(){
		val conf = new SparkConf().setAppName("basic-read").setMaster("local")
		val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val path = "D:/scalaTry/data/prac1.csv"
    val df = sqlContext.read.format("csv").option("header","true").load(path)
    var df1=df.withColumn("words",split(col("words"),","))
    var df2=df1.withColumn("words",explode(col("words")))
    var df3=df2.withColumn("combined",col("id")).groupBy("words").agg(collect_list(col("combined")) as "ans")
    var df4=df1.select("word")
    var finalDf= df4.join(df3,df3("words")===df4("word"),"inner")
    finalDf=finalDf.drop("words")
    finalDf.show()
    sc.stop()
  }
  def evaluatePractice2(){
		val conf = new SparkConf().setAppName("basic-read").setMaster("local")
		val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val path = "D:/scalaTry/data/prac2.csv"
    val df = sqlContext.read.format("csv").option("header","true").load(path)
    var df1=df.groupBy("group").agg(max("id") as "id")
    df1.show()
    var df2=df.groupBy("group").agg(collect_list("id") as "id")
    df2.show()
    var df3=df.groupBy("group").agg(max("id") as "max_id", min("id") as "min_id")
    df3.show()
    sc.stop()
  }

}