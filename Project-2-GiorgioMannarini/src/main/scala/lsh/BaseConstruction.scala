package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //build buckets here
  private val minHash = new MinHash(seed)
  private val buckets = minHash.execute(data).map(row => (row._2, Set(row._1))).reduceByKey((r1, r2)=> r1.union(r2))

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    val queriesHashed = minHash.execute(queries)
    queriesHashed.map(query => {
      (query._2, query._1)
    }).join(buckets).map(joined => joined._2)
  }
}
