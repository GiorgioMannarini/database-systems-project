package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
                       input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
                       collation: RelCollation,
                       offset: Option[Int],
                       fetch: Option[Int]
                     ) extends skeleton.Sort[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](input, collation, offset, fetch)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  // private var sorted = List[Tuple]() Commented, I don't need it
  var table: IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  var resultIndex: Int = 0 //Index of the tuple to return
  var resultLength: Int = 0 //Length of the result (considering offset and fetch)
  var resultFetch: Int = -1 //It is fetch when it's defined, -1 otherwise (default value to tell there's no fetch)
  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    //Initializations
    if (fetch.isDefined){
      resultFetch = fetch.get
      if (resultFetch == 0){
        //If resultFetch is 0 it means we have nothing to return. The next logic is useless
        return
      }
    }

    //If I have an offset I have to start returning the tuples from that offset
    if (offset.isDefined) resultIndex = offset.get

    //Reverting the collation to apply the ORDER BY clauses correctly when they're more than 1
    var reverseCollation : List[RelFieldCollation] = List[RelFieldCollation]()
    collation.getFieldCollations.forEach(el => {
      reverseCollation = reverseCollation.+:(el)
    })

    input.open()

    //Creating the input table to sort
    var row = input.next()
    while (row != NilTuple) {
      table :+= row.get
      row = input.next()
    }

    //Logic: I need to apply the ORDER BY clauses in reverse order, one after the other, to have the correct result.
    //So, after building the reverseCollation, for each clause I get the direction (ASC, DESC) and apply the scala
    //sortWith function, that takes two inputs (two rows) and sort them according to the function provided.
    //In the function i check the direction.
    reverseCollation.foreach(coll => {
      val order = coll.getDirection
      table = table.sortWith((r1, r2) => {
        order match {
          case RelFieldCollation.Direction.ASCENDING =>
            RelFieldCollation.compare(r1(coll.getFieldIndex).asInstanceOf[Comparable[_]], r2(coll.getFieldIndex).asInstanceOf[Comparable[_]], 0) < 0
          case RelFieldCollation.Direction.DESCENDING =>
            RelFieldCollation.compare(r1(coll.getFieldIndex).asInstanceOf[Comparable[_]], r2(coll.getFieldIndex).asInstanceOf[Comparable[_]], 0) > 0
        }
      })
    })

    resultLength = table.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    //If resultFetch is 0 I finished the tuples that I had to return (supposing that fetch was defined),
    //That's why I check this clause too
    if (resultIndex >= resultLength || resultFetch == 0){
      return NilTuple
    }
    val nextTuple = table(resultIndex)
    resultFetch -=1 //I returned a tuple, I should decrease the number of tuples I still have to return
    resultIndex +=1 //Normal index logic of all the other next functions
    Some(nextTuple)
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}

