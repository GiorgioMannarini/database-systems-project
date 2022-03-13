package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  var finalResult: IndexedSeq[Tuple] = IndexedSeq[Tuple]() // This tuple will contain the final result
  var finalLength: Int = 0 //Final result length
  var resultIndex: Int = 0 //Index that will help me to return the result tuple by tuple

  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    //Initializations
    var table: IndexedSeq[Tuple] = IndexedSeq[Tuple]()
    input.open()

    //Creating the table
    var tableRow = input.next()
    while (tableRow != NilTuple) {
      table :+= tableRow.get
      tableRow = input.next()
    }

    //Case 1: the table to aggregate is empty
    if (table.isEmpty) {

      //Intermediate result is a Tuple, it will contain the empty value for each agg operator
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
        table.foreach(row => {
          consideredValues :+= agg.getArgument(row)
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
      var groupList: List[Int] = List[Int]() //To transform the groupset into a Scala list
      groupSet.forEach(index => {
        groupList :+= index
      })

      //This built in scala function creates a Map. Each element of the map is a group of the GROUP BY clause
      //The keys are the fields used to group.
      val groups = table.groupBy(tableRow => groupList.map(tableRow))

      //For each group I need to apply all the agg operators
      groups.foreach(group => {

        //intermediateResult stores the result of all the agg operators for one group
        var intermediateResult: List[Any] = List[Any]()
        aggCalls.foreach(agg => {
          var consideredValues: IndexedSeq[Any] = IndexedSeq[Any]()
          group._2.foreach(row => {
            consideredValues :+= agg.getArgument(row)
          })
          intermediateResult :+= consideredValues.reduce(agg.reduce)
        })

        //The logic is the same as before, but now I have a result (tuple) for each group.
        //I append the tuole to the final result
        finalResult :+= (group._1 ::: intermediateResult).toIndexedSeq
      })
      finalLength = finalResult.size
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    var nextTuple: Tuple = IndexedSeq[Any]()
    if (resultIndex < finalLength) {
      nextTuple = finalResult(resultIndex)
      resultIndex += 1
      Some(nextTuple)
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
