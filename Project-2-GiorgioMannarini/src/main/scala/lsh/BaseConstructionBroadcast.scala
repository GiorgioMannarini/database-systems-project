package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  private val minHash = new MinHash(seed)

  // Unfortunately I have to collect the data on the driver, as RDDs can't be broadcasted
  private val buckets = minHash.execute(data).map(row => (row._2, Set(row._1))).reduceByKey((r1, r2)=> r1.union(r2))
    .collect().toMap.withDefaultValue(Set[String]())

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    val broadcastedBuckets = sqlContext.sparkContext.broadcast(buckets)
    val queriesHash = minHash.execute(queries)

    queriesHash.map(query => {
      val movies_found = broadcastedBuckets.value(query._2)
      (query._1, movies_found)
    })
  }
}
