package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import java.io.File


object Main {
  def generate(sc : SparkContext, input_file : String, output_file : String, fraction : Double) : Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_recall = recall_vec._1/recall_vec._2

    avg_recall
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_precision = precision_vec._1/precision_vec._2

    avg_precision
  }

  def construction1(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction1 composition here
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 42)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 43)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 44)
    val lsh4 =  new BaseConstruction(SQLContext, rdd_corpus, 45)
    new ANDConstruction(List(lsh1, lsh2, lsh3, lsh4))
  }

  def construction2(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 42)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 43)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 44)
    val lsh4 =  new BaseConstruction(SQLContext, rdd_corpus, 45)
    val lsh5 =  new BaseConstruction(SQLContext, rdd_corpus, 46)
    new ORConstruction(List(lsh1, lsh2, lsh3, lsh4, lsh5))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    



    /*val p = prova.map(el => {
      if (el >= 0 & el < 10){
        (1, el)
      }
      else (2, el)
    }).
      partitionBy(new HashPartitioner(2)).mapPartitions(x => {
      val l = x.toList
      List((l(0)._1, l.size)).toIterator
    }).collect()

    //p.collect().foreach(pa => print(pa +"\n"))
    p.foreach(print)*/



    /*val conf = new SparkConf()
      .setAppName("app")
      .set("spark.executor.memory", "16G")
      .set("spark.shuffle.file.buffer", "16M")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sc.setLogLevel("OFF")

    val corpus_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-skewplusplus.csv/part-00000"

    val rdd_corpus_fragment = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .repartition(2)
      .cache()

    val rdd_corpus = ((1 to 10).map(x => rdd_corpus_fragment).reduce(_ ++ _)).repartition(2)

    val query_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-skewplusplus.csv/part-00000"
    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .repartition(2)
      .cache()

    println(rdd_corpus.count() + rdd_query.count())

    val lsh1 = new BaseConstruction(sqlContext, rdd_corpus, 42)
    val lsh2 = new BaseConstructionBalanced(sqlContext, rdd_corpus, 42, 2)

    for (i <- 1 to 10) {
      val t1 = System.nanoTime

      val res2 = lsh1.eval(rdd_query).flatMap(x => x._2).count()

      val duration1 = (System.nanoTime - t1) / 1e9d

      val t2 = System.nanoTime

      val res1 = lsh2.eval(rdd_query).flatMap(x => x._2).count()

      val duration2 = (System.nanoTime - t2) / 1e9d

      println(duration1)
      println(duration2)
    }*/






   /* // DEFINING CORPUS AND QUERY
    val corpus_file = new File(getClass.getResource("/corpus-20.csv/part-00000").getFile).getPath
    //val corpus_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
    val rdd_corpus = sqlContext.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-1-10.csv/part-00000").getFile).getPath
    //val query_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"

    val rdd_query = sqlContext.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .sample(false, 1)


    println("Number of queries " + rdd_query.count())
    println("Number of movies "+rdd_corpus.count())
    //val exactNN = new ExactNN(sqlContext=sqlContext, data = rdd_corpus, threshold = 0.3)
    val baseConstruction = new BaseConstruction(sqlContext = sqlContext, data = rdd_corpus, seed = 42)
    val baseConstructionBalanced = new BaseConstructionBalanced(sqlContext = sqlContext, data = rdd_corpus,
      seed = 42, partitions = 8)
    val baseConstructionBroadcast = new BaseConstructionBroadcast(sqlContext = sqlContext, data = rdd_corpus, seed = 42)

    val tExact = System.nanoTime
    //val resExact = exactNN.eval(rdd_query).collect()
    val durationExact = (System.nanoTime - tExact) / 1e92

    val tBase = System.nanoTime
    val resBase = baseConstruction.eval(rdd_query).collect()
    val durationBase = (System.nanoTime - tBase) / 1e9d

    val tBalanced = System.nanoTime
    val resBalanced = baseConstructionBalanced.eval(rdd_query).collect()
    val  durationBalanced = (System.nanoTime - tBalanced) / 1e9d

    val tBroadcast = System.nanoTime
    val resBroadcast = baseConstructionBroadcast.eval(rdd_query).collect()
    val  durationBroadcast = (System.nanoTime - tBroadcast) / 1e9d

    println("Exact: "+durationExact+"\tBase: "+durationBase+"\tBalanced: "+durationBalanced+"\tBroadcast: "+durationBroadcast)

    // PRECISION AND RECALL
    //val exactNoCollect = exactNN.eval(rdd_query)
    val baseNoCollect = baseConstruction.eval(rdd_query)
    val balancedNoCollect = baseConstructionBalanced.eval(rdd_query)
    val broadcastNoCollect = baseConstructionBroadcast.eval(rdd_query)
    val andNoCollect = construction1(sqlContext, rdd_corpus).eval(rdd_query)
    val orNoCollect = construction2(sqlContext, rdd_corpus).eval(rdd_query)

    //val precBase = precision(exactNoCollect, baseNoCollect)
    //val precBalanced = precision(exactNoCollect, balancedNoCollect)
    //val precBroadcast = precision(exactNoCollect, broadcastNoCollect)
    //val precAnd = precision(exactNoCollect, andNoCollect)
    //val precOr = precision(exactNoCollect, orNoCollect)

    println("PRECISIONS")
    //println("Base: "+precBase+" Balanced: "+precBalanced+" Broadcast: "+precBroadcast +" AND: "+precAnd +" OR: "+precOr)

    val recBase = recall(exactNoCollect, baseNoCollect)
    val recBalanced = recall(exactNoCollect, balancedNoCollect)
    val recBroadcast = recall(exactNoCollect, broadcastNoCollect)
    val recAnd = recall(exactNoCollect, andNoCollect)
    val recOr = recall(exactNoCollect, orNoCollect)

    println("RECALLS")
    println("Base: "+recBase+" Balanced: "+recBalanced+" Broadcast: "+recBroadcast+" AND: "+recAnd +" OR: "+recOr)

    // LOGIC FOR THE AVERAGE DISTANCE. I USE JACCARD DISTANCE

    val rddQueryCollected = rdd_query.collectAsMap()
    val corpusCollected = rdd_corpus.collectAsMap()
    println("Base distance")
    println(computeDistance(rddQueryCollected, corpusCollected, resBase))
    //println(computeDistance(rddQueryCollected, corpusCollected, resBalanced))
    //println(computeDistance(rddQueryCollected, corpusCollected, resBroadcast))
    println("Exact distance")
    println(computeDistance(rddQueryCollected, corpusCollected, resExact))
    println("And distance")
    println(computeDistance(rddQueryCollected, corpusCollected, andNoCollect.collect()))
    println("Or distance")
    println(computeDistance(rddQueryCollected, corpusCollected, orNoCollect.collect()))*/

  }

  private def computeJaccard(keyword1: List[String], keyword2: List[String]): Double = {
    val s1 = keyword1.toSet
    val s2 = keyword2.toSet

    s1.intersect(s2).size.asInstanceOf[Double] / s1.union(s2).size.asInstanceOf[Double]
  }

  def transpose(matrix: RDD[(Int, Int, Double)]): RDD[(Int, Int, Double)] = {

  }

  private def computeDistance(rddQueryCollected: scala.collection.Map[String, List[String]],
                              corpusCollected: scala.collection.Map[String, List[String]],
                              resultOfConstruction: Array[(String, Set[String])]) : Double = {

    val resArray : Array[Double] = resultOfConstruction.map(rb => {
      val query = rb._1
      val associatedMovies = rb._2
      val queryKeywords = rddQueryCollected.getOrElse(query, List[String]())
      var totalQueryDistance : Double = 0

      associatedMovies.foreach(title => {
        val movieKeywords = corpusCollected.getOrElse(title, List[String]())
        val jaccDistance = 1- computeJaccard(queryKeywords, movieKeywords)
        totalQueryDistance += jaccDistance
      })
      totalQueryDistance = totalQueryDistance / associatedMovies.size
      totalQueryDistance
    })
    resArray.sum / resArray.length
  }

}
