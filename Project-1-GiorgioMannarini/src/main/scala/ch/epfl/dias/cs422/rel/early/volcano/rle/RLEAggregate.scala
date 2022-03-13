package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  var finalResult: IndexedSeq[Tuple] = IndexedSeq[Tuple]() // This tuple will contain the final result
  var finalLength: Int = 0 //Final result length
  var resultIndex: Int = 0 //Index that will help me to return the result tuple by tuple

  override def open(): Unit = {
    //Initializations
    var table: IndexedSeq[RLEentry] = IndexedSeq[RLEentry]()
    input.open()

    //Creating the table of RLE entries
    var nextRLE = input.next()
    while (nextRLE != NilRLEentry) {
      table :+= nextRLE.get
      nextRLE = input.next()
    }

    //Case 1: the table to aggregate is empty
    if (table.isEmpty) {
      var intermediateResult: Tuple = IndexedSeq[Any]()
      aggCalls.foreach(agg => {
        intermediateResult :+= aggEmptyValue(agg)
      })
      //Final result: a tuple of tuples (in this case one). Will be returned by next() row by row
      finalResult :+= intermediateResult
      finalLength = 1
    }

    //Case 2: I have a non empty table, but there's no GROUP BY clause
    else if (groupSet.isEmpty) {
      //Intermediate result is a Tuple, it will contain the result for each agg operator
      var intermediateResult: Tuple = IndexedSeq[Any]()
      aggCalls.foreach(agg => {
        var consideredValues: IndexedSeq[Any] = IndexedSeq[Any]()
        table.foreach(RLE => {
          // Get argument for RLE entry. I build the table with only the considered values
          consideredValues :+= agg.getArgument(RLE.value, RLE.length)
        })
        //For each row I take the fields to consider, and I apply the agg function.
        //This is done for each agg function (as you can see by the nested loops)
        intermediateResult :+= consideredValues.reduce(agg.reduce)
      })
      // The final result, even in this case, will be a tuple containing only one tuple (no groupby)
      finalResult :+= intermediateResult
      finalLength = 1
    }

    //Case 3: I have a non empty table and there's a GROUP BY in the query
    else {
      //I need the groupset to be a scala list for my logic
      val groupList = groupSet.asScala.toIndexedSeq

      //This built in scala function creates a Map. Each element of the map is a group of the GROUP BY clause
      //The keys are the fields used to group.
      val groups = table.groupBy(RLE => groupList.map(index => RLE.value(index)))

      //For each group I need to apply all the agg operators
      groups.foreach(group => {
        //intermediateResult stores the result of all the agg operators for one group
        var intermediateResult: IndexedSeq[Any] = IndexedSeq[Any]()
        aggCalls.foreach(agg => {
          var consideredValues: IndexedSeq[Any] = IndexedSeq[Any]()
          group._2.foreach(RLE => {
            consideredValues :+= agg.getArgument(RLE.value, RLE.length)
          })
          intermediateResult :+= consideredValues.reduce(agg.reduce)
        })

        //The logic is the same as before, but now I have a result (tuple) for each group.
        //I append the tuple to the final result
        finalResult :+= (group._1 ++ intermediateResult)
      })
      finalLength = finalResult.size
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {

    if (resultIndex < finalLength) {
      val nextRLE = RLEentry(resultIndex, 1, finalResult(resultIndex))
      resultIndex += 1
      Some(nextRLE)
    } else {
      NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
