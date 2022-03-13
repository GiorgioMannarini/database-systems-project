package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here
    rdd.cartesian(data).map(r =>{
      val keyword1 = r._1._2
      val keyword2 = r._2._2
      val similarity = computeJaccard(keyword1, keyword2)
      (r._1._1, (r._2._1, similarity))
    }).filter(row => {
      row._2._2 > threshold
    }).map(filteredRow => {
      (filteredRow._1, Set(filteredRow._2._1))
    }).reduceByKey((filteredRow1, filteredRow2) => {
      filteredRow1.union(filteredRow2)
    })
  }

  private def computeJaccard(keyword1: List[String], keyword2: List[String]): Double = {
    val s1 = keyword1.toSet
    val s2 = keyword2.toSet
    s1.intersect(s2).size.asInstanceOf[Double] / s1.union(s2).size.asInstanceOf[Double]
  }
}
