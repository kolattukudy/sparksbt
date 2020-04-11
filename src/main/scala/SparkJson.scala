import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
object SparkJson extends App{

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("spark session example")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._

  val df = sparkSession.read.option("multiLine", true).json("customer.json")
  df.show(false)
  import org.apache.spark.sql.functions.col
  def collectFields(field: String, sc: DataType): Seq[String] = {
    sc match {
      case sf: StructType => sf.fields.flatMap(f => collectFields(field+"."+f.name, f.dataType))
      case _ => Seq(field)
    }
  }

  @tailrec
  def recurs(df: DataFrame): DataFrame = {
    if(df.schema.fields.find(_.dataType match {
      case ArrayType(StructType(_),_) | StructType(_) => true
      case _ => false
    }).isEmpty) df
    else {
      val columns = df.schema.fields.map(f => f.dataType match {
        case _: ArrayType => explode(col(f.name)).as(f.name)
        case s: StructType => col(s"${f.name}.*")
        case _ => col(f.name)
      })
      recurs(df.select(columns:_*))
    }
  }
  val recursedDF = recurs(df)
  val valuesColumns = recursedDF.columns.filter(_.startsWith("customer"))
  val projectionDF = recursedDF.withColumn("values", coalesce(valuesColumns.map(col):_*))
  projectionDF.show(false)



  val fields = collectFields("",df.schema).map(_.tail)

 val seconddf= df.select(fields.map(col):_*)
  seconddf.show(false)

  seconddf.select($"customerid",$"customerstatus",explode($"persons").as("persons"))
    .select($"customerid",$"customerstatus",$"persons.*")
    .show(false)
}
