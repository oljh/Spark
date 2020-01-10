package Tasks

import org.apache.spark.sql._
import org.elasticsearch.spark.sql._

import scala.util.matching.Regex

case class AccLog(timestamp: String, level: String, ip: String, user: String, method: String, path: String, protocol: String)

object ch7_load_save_data_t2 {
  def main(args: Array[String]) {

   // val conf = new SparkConf().setMaster("local[*]").setAppName("WriteToES")


    val elasticConf: Map[String, String] = Map(
      "es.nodes" -> "es-svc.elasticsearch.svc.cluster.local",
      "es.port" -> "9200",
      "es.resource" -> "olagutin_spark_access_log/logline",
      "es.mapping.id" -> "timestamp",
      "es.write.operation" -> "upsert",
      "es.index.auto.create" -> "true",
      "es.net.ssl" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.net.http.auth.user" -> "admin",
      "es.net.http.auth.pass" -> "admin"
    )

    val spark = SparkSession.builder.appName("rddLog").master("local").getOrCreate()
    val accsFile = spark.read.textFile("src/res/access.log").rdd
    import spark.implicits._


    val accsPattern: Regex = ("^\\[" +
      "(?<timestamp>(\\d{2}\\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\/\\d{4}\\s+\\d{2}:\\d{2}:\\d{2}\\s+-\\d{4})\\])\\s+" +
      "(?<level>(?>INFO|ERROR|WARN|TRACE|DEBUG|FATAL))\\s+" +
      "(?<ip>((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.?){4})\\s+" +
      "(?<user>.+)\\s+-\\s+\"" +
      "(?<method>(POST|HEAD|PUT|GET|DELETE))\\s+" +
      "(?<path>(.*?))\\s+" +
      "(?<protocol>(.*?))\"" +
      "(.+)?$").r

    val accsDataRdd = accsFile
      .flatMap(accsPattern.findAllMatchIn(_))
      .map(m => AccLog(m.group(2), m.group(4), m.group(5), m.group(8), m.group(9), m.group(12), m.group(14)))
      .toDF()

    accsDataRdd.saveToEs("olagutin_spark_access_log/logline", elasticConf)
  }


}
