package Tasks

import org.apache.spark._
import org.elasticsearch.spark._

object testSEl {
  /** Simple application to store data in ElasticSearch from Apache spark using Scala */


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkToElasticSearch")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "localhost")
      .set("es.port", "9200")
      .set("es.http.timeout", "5m")

      .set("es.scroll.size", "50")
    val sc = new SparkContext(conf)


    val numbers = Map("One" -> 1, "Two" -> 2, "Three" -> 3)
    val alpha = Map("A" -> "Apple", "B" -> "Ball", "C" -> "Cat")

    sc.makeRDD(Seq(numbers, alpha)).saveToEs("someres/alphabets")

  }

}
