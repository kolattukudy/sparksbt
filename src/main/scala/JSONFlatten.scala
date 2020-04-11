import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

object JSONFlatten extends App{


  val sparkSession = SparkSession.builder
    .master("local")
    .appName("spark session example")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._

  val json_string = """{
                   "Total_Value": 3,
                   "Topic": "Example",
                   "values": [
                              {
                                "value1": "#example1",
                                "points": [
                                           [
                                           "123",
                                           "156"
                                          ]
                                    ],
                                "properties": {
                                 "date": "12-04-19",
                                 "model": "Model example 1"
                                    }
                                 },
                               {"value2": "#example2",
                                "points": [
                                           [
                                           "124",
                                           "157"
                                          ]
                                    ],
                                "properties": {
                                 "date": "12-05-19",
                                 "model": "Model example 2"
                                    }
                                 }
                              ]
                       }"""




  def flattenDataframe(df: DataFrame): DataFrame = {
    //getting all the fields from schema
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    //length shows the number of fields inside dataframe
    val length = fields.length
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldName1 = fieldName
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName1)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName1) as $fieldName1")
          //val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName1.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataframe(explodedDf)

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }
  val parlleized=sparkSession.sparkContext.parallelize(json_string)
  val df = sparkSession.sqlContext.read.json(Seq(json_string).toDS())
  df.show(false)
  val splitzDF=flattenDataframe(df)
  splitzDF.show(false)

}
