package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, RLEColumn, RLEentry, Tuple}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}


import scala.jdk.CollectionConverters.ListHasAsScala

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {


  // NOTE: This implementation is really similar to what has been shown in the video.
  // Why changing something that works very well? But I improved it a little bit
  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )
  private val prog = getRowType.getFieldList.asScala.map(_ => 0)
  private var current = 0

  private val rles = getRowType.getFieldList.asScala.map(
    e => scannable.asInstanceOf[RLEStore].getRLEColumn(e.getIndex))

  /**
   * Helper function (you do not have to use it or implement it)
   * It's purpose is to show how to convert the [[scannable]] to a
   * specific [[Store]].
   *
   * @param rowId row number (startign from 0)
   * @return the row as a Tuple
   */
  private def getRow(rowId: Int): Tuple = {
    getRowType.getFieldList.asScala.map(
      f => {
        val colIndex = f.getIndex
        while(rles(colIndex)(prog(colIndex)).endVID < rowId){
          prog(colIndex) +=1
        }
        rles(colIndex)(prog(colIndex)).value
      }
    ).reduce((a, b) => a ++ b)
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    current = 0
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if(current < scannable.getRowCount){
      val res = getRow(current)
      current +=1
      Some(res)
    }
    else{
      NilTuple
    }

  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}
}
