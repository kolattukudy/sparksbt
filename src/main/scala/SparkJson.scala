import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

object SparkJson extends App{

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("spark session example")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

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

  df.select(fields.map(col):_*).show(false)
}
