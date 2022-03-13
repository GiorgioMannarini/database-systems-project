package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    // Initializations
    var inputTable = input.execute()
    val inputVector = inputTable.last.asInstanceOf[IndexedSeq[Boolean]]
    inputTable = inputTable.dropRight(1)

    // If the input is empty I don't need to go through the following logic
    if (inputTable.isEmpty){
      return IndexedSeq[Column]()
    }

    // Number of rows to consider
    val numRows = inputVector.count(el => el)

    // If fetch is not defined, then it is equal to the number of rows in the input vector,
    // Since here I don't drop any row
    val resultFetch = numRows
    if (fetch.isDefined){
      val resultFetch = fetch.get
      if (resultFetch == 0){
        //If resultFetch is 0 it means we have nothing to return. The next logic is useless
        return IndexedSeq[Column]()
      }
    }

    // If offset is not defined, it's zero
    var resultOffset = 0
    if (offset.isDefined) {
      resultOffset = offset.get
    }

    // CREDS: https://stackoverflow.com/questions/39514128/apply-boolean-mask-to-scala-array
    // Take only the "true" rows if there are false rows
    if (numRows < inputVector.size) inputTable = inputTable.
      map(col => col.zip(inputVector).filter(_._2).map(_._1))

    // In this case it could be empty because all the rows have to be dropped
    if (inputTable.isEmpty){
      return IndexedSeq[Column]()
    }

    //Reverting the collation to apply the ORDER BY clauses correctly when they're more than 1
    var reverseCollation : List[RelFieldCollation] = List[RelFieldCollation]()
    collation.getFieldCollations.forEach(el => {
      reverseCollation = reverseCollation.+:(el)
    })

    reverseCollation.foreach(coll => {
      // I need the indexes too, for the other columns
      val column = inputTable(coll.getFieldIndex).zipWithIndex
      val order = coll.getDirection
      val indexes = column.sortWith((r1, r2) => {
        order match {
          case RelFieldCollation.Direction.ASCENDING =>
            RelFieldCollation.compare(r1._1.asInstanceOf[Comparable[_]], r2._1.asInstanceOf[Comparable[_]], 0) < 0
          case RelFieldCollation.Direction.DESCENDING =>
            RelFieldCollation.compare(r1._1.asInstanceOf[Comparable[_]], r2._1.asInstanceOf[Comparable[_]], 0) > 0
        }
      }).map(_._2) //Taking only the re-sorted indexes

      // Sorting all the columns
      inputTable = inputTable.map(col => indexes.map(col))
    })

    // Output vector according to fetch and offset
    val outputVector = IndexedSeq.fill(resultOffset)(false) ++ IndexedSeq.fill(resultFetch)(true) ++ IndexedSeq.fill(numRows - (resultFetch + resultOffset))(false)
    inputTable :+= outputVector
    inputTable

  }
}
