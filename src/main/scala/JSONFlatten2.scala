import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JSONFlatten2 extends App{


  val sparkSession = SparkSession.builder
    .master("local")
    .appName("spark session example")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._

  val df = Seq(
    (1, "Luke", Some(Array("baseball", "soccer"))),
    (2, "Lucy", None),
    (3, "Sam", Some(Array("")))
  ).toDF("id", "name", "likes")


  df.show(false)
  //df.select( $"id",$"name", explode($"likes")).show(false)

  df.withColumn("likes", explode(
    when(col("likes").isNotNull, col("likes"))
      // If null explode an array<string> with a single null
      .otherwise(array(lit(null).cast("string"))))).show(false)

  def explodeOuter(df: Dataset[Row], columnsToExplode: List[String]) = {
    val arrayFields = df.schema.fields
      .map(field => field.name -> field.dataType)
      .collect { case (name: String, type_val: ArrayType) => (name, type_val.asInstanceOf[ArrayType]) }
      .toMap
    println(arrayFields)
    columnsToExplode.foldLeft(df) { (dataFrame, arrayCol) =>
      dataFrame.withColumn(arrayCol, explode(when((col(arrayCol).isNotNull) , col(arrayCol))
        .otherwise(array(lit(null).cast(arrayFields(arrayCol).elementType)))))
    }
  }

  explodeOuter(df,List("likes")).show(false)
}

