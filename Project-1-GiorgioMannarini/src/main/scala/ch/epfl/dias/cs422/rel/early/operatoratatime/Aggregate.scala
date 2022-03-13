package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    val inputTable = input.execute()
    var result : IndexedSeq[Column] = IndexedSeq[Column]()

    // If the input is empty we don't need to return anything
    if (inputTable.isEmpty){
      aggCalls.foreach(agg => {
        result :+= IndexedSeq(aggEmptyValue(agg))
      })
      result :+= IndexedSeq(true)
      return result
    }

    //Building the input table only with the rows that are actually considered and checking if this one is empty
    val consideredTable = inputTable.transpose.filter(row => row.last.asInstanceOf[Boolean])
    if (consideredTable.isEmpty){
      aggCalls.foreach(agg => {
        result :+= IndexedSeq(aggEmptyValue(agg))
      })
      result :+= IndexedSeq(true)
      return result
    }

    // I have no groups but the table is not empty
    else if (groupSet.isEmpty){
      aggCalls.foreach(agg => {
        var consideredValues: IndexedSeq[Any] = IndexedSeq[Any]()
        consideredTable.foreach(row => {
          consideredValues :+= agg.getArgument(row)
        })
        //For each row I take the fields to consider, and I apply the agg function.
        //This is done for each agg function (as you can see by the nested loops)
        result :+= IndexedSeq(consideredValues.reduce(agg.reduce))
      })
      // Creating the input vector
      result :+= IndexedSeq.fill(result(0).size)(true)
      return result
    }

    // I have also groups
    else {
      //I need the groupset to be a scala list for my logic
      val groupList = groupSet.asScala.toIndexedSeq
      //This built in scala function creates a Map. Each element of the map is a group of the GROUP BY clause
      //The keys are the fields used to group.
      val groups = consideredTable.groupBy(tableRow => groupList.map(index => tableRow(index)))
      result ++= groups.keys.toIndexedSeq.transpose

      aggCalls.foreach(agg => {
        var intermediateResult: IndexedSeq[Any] = IndexedSeq[Any]()
        groups.foreach(group => {
          var consideredValues: IndexedSeq[Any] = IndexedSeq[Any]()
          group._2.foreach(row => {
            consideredValues :+= agg.getArgument(row)
          })
          intermediateResult :+= consideredValues.reduce(agg.reduce)
        })
        result :+= intermediateResult
        })
    }
    result :+= IndexedSeq.fill(result(0).size)(true)
    result
  }
}
