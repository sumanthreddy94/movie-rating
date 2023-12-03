import Main.processDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class MovieStatisticsSpec extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder().appName("test").master("local[1]").getOrCreate();

  test("processDataFrame should calculate mean and stddev of 'rating' column") {
    val testData = Seq((1, 4.0), (2, 3.5), (3, 2.0), (4, 5.0))
    val schema = List("userId", "rating")
    val testDf = spark.createDataFrame(testData).toDF(schema: _*)
    val resultDf = processDataFrame(testDf);
    // Add assertions based on your expected DataFrame
    resultDf.filter(col("summary") === "mean").select("rating").first().getAs[Double](0) shouldBe "3.625"
  }
}
