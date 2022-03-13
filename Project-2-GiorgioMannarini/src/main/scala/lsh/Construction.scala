package lsh

import org.apache.spark.rdd.RDD

trait Construction {
  def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])]
}
