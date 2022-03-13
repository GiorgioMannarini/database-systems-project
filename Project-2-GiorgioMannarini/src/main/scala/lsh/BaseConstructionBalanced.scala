package lsh


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  private val minHash = new MinHash(seed)
  private val buckets = minHash.execute(data).map(row => (row._2, Set(row._1))).reduceByKey((r1, r2)=> r1.union(r2)).cache()


  def computeMinHashHistogram(queries : RDD[(String, Int)]) : Array[(Int, Int)] = {
    // Histogram logic: the first number is the bucket identifier (min hash of the bucket). The second number is the
    // number of queries related to that bucket. I sort the histograms (ascending) so that it is easier, later,
    // to create the partition boundaries
    queries.map(query => {
      (query._2, 1)
    }).reduceByKey((q1, q2) => q1+q2).sortByKey().collect()
  }

  def computePartitions(histogram : Array[(Int, Int)]) : Array[Int] = {
    // The logic here is simple: first of all, I need the total number of queries
    val queryNum: Double = histogram.map(h => h._2).sum

    // Now the goal is to have an equal (more or less, it won't be equal every time) number of queries for each
    // partition
    val queriesForPartition = (queryNum/partitions).ceil

    // I need the partition boundaries: given the histogram, these boundaries define
    // when a partition ends. All the buckets between one boundary and another will end up in one partition, with all
    // their queries. A boundary is last_bucket_id (for that partition) +1
    var partitionsBoundaries: List[Int] = List()
    var sum = 0
    val lastId = histogram.last._1
    histogram.foreach(element => {
      sum += element._2
      if (sum >= queriesForPartition || element._1 ==lastId) {
        partitionsBoundaries :+= element._1 +1
        sum = 0
      }
    })
    partitionsBoundaries.toArray
  }



  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // Handling the case for which we do not have queries
    if (queries.isEmpty){
      return sqlContext.sparkContext.emptyRDD
    }

    val queriesHash = minHash.execute(queries)
    val histogram = computeMinHashHistogram(queriesHash)
    val partitionsBoundaries = computePartitions(histogram)

    // Now I assign each bucket and query to its partition and then join the two rdds
    var pid = 0
    var boundary = partitionsBoundaries(pid)
    val rddOfBucketsPartitioned = buckets.map(bucket => {
      pid = 0
      partitionsBoundaries.foreach(el => {
        if (bucket._1 >= el){
          pid +=1
        }
      })
      (pid, bucket)
    }).groupByKey()

    pid = 0
    boundary = partitionsBoundaries(pid)
    val rddOfQueriesPartitioned  = queriesHash.map(query => {
      pid = 0
      partitionsBoundaries.foreach(el => {
        if (query._2 >= el){
          pid +=1
        }
      })
      (pid, query)
    }).groupByKey()

    val finalPartitions = rddOfBucketsPartitioned.join(rddOfQueriesPartitioned)

    // Finally, for each partition I take all the queries and the buckets, and obtain a result for each query.
    finalPartitions.flatMap(partition => {
      val queries = partition._2._2
      val buck = partition._2._1.toMap.withDefaultValue(Set[String]())
      queries.map(query => {
        (query._1, buck(query._2))
      })
    })

  }
}