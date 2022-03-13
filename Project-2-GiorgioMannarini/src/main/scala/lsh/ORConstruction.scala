package lsh

import org.apache.spark.rdd.RDD

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    children.map(const => const.eval(rdd)).reduce((rdd1, rdd2) => {
      // Distinct done because we have duplicate queries, so in the end the number of queries is always equal
      // To the one we have in input
      rdd1.join(rdd2.distinct()).map(row => {
        (row._1, row._2._1.union(row._2._2))
      })
    })
  }
}
