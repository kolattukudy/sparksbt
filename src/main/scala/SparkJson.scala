import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions._
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

  val fields = collectFields("",df.schema).map(_.tail)

 val seconddf= df.select(fields.map(col):_*)
  seconddf.show(false)

  seconddf.select($"customerid",$"customerstatus",explode($"persons").as("persons"))
    .select($"customerid",$"customerstatus",$"persons.*")
    .show(false)
}
