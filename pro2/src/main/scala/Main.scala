import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object workRead {

  def main(args:Array[String]){
    evaluatePractice1()
  }

  def evaluatePractice1(){
		val conf = new SparkConf().setAppName("pokemon-read").setMaster("local")
		val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val path = "D:/scalaTry/data/Pokemon.csv"
    var df=sqlContext.read.format("csv").option("header","true").load(path)
    // type 1 count
    var df2=df.groupBy(col("Type 1")).agg(count("Type 1") as "countq1")
    // type 2 count 
    var df3=df.groupBy(col("Type 2")).agg(count("Type 2") as "countq2")
    // sums category wise
    df=df.withColumnRenamed("Sp. Atk", "SpAtk")
    df=df.withColumnRenamed("Sp. Def", "SpDef")
    var df4=df.groupBy(col("Type 1")).agg(sum("HP") as "HP",sum("Attack") as "Attack",sum("Defense") as "Defense",sum("SpAtk") as "SpAtk",sum("SpDef") as "SpDef",sum("Speed") as "Speed")
    df4.show()
    // top 5 members-pokemon
    var df5=df.orderBy(col("Total").desc).show(5)
    // legendary count
    var df6=df.filter(df("Legendary")==="TRUE")
    println("Legendary Count - ", df6.count)
    println("Total Count - ", df.count)
    // various Generations
    println("Generation Count - ", df.select("Generation").distinct.count)
    df.select("Generation").distinct.show()
    df.groupBy("Generation").agg(count("id") as "countOfPokemons").show()
    var df7=df.groupBy("Generation").agg(max("Total") as "power",collect_list("id") as "id")
    var df8=df7.withColumn("id",explode(col("id")))
    var df10=df.select("id","Total","Name")
    df10=df10.withColumnRenamed("id","uid")
    var df9=df8.join(df10,(df10("uid")===df8("id")) && (df10("Total")===df8("power")))
    df9=df9.drop("uid")
    df9=df9.drop("Total")
    df9=df9.dropDuplicates()
    df9.show()
    sc.stop()
  }
}