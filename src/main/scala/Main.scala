import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {
  val ratingsSrc = "src/in/ratings_small.csv";
  val spark: SparkSession = SparkSession.builder().appName("Movies Statistics").master("local[1]").getOrCreate();

  def getDataFrame(fileSrc: String, conf: Map[String, String] = Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true")): DataFrame = spark.read.options(conf).csv(fileSrc).toDF();
  val ratingsDf = getDataFrame(ratingsSrc);

  def processDataFrame(dataFrame: DataFrame): DataFrame = {
    val statsDf = dataFrame.describe("rating");
    statsDf.filter(col("summary") === "mean" or col("summary") === "stddev");
  }

  processDataFrame(ratingsDf).show();
}