package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, RLEentry, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Decode[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var result : IndexedSeq[IndexedSeq[Any]] = IndexedSeq[IndexedSeq[Any]]()
  private var resSize : Int = 0
  private var resIndex : Int = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    var nextRLE = input.next()
    while (nextRLE != NilTuple){
      val temp = (1 to nextRLE.get.length.toInt).map( _ => nextRLE.get.value)
      temp.foreach(res =>{
        result :+= res
      })
      nextRLE = input.next()
    }
    resSize = result.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (resIndex < resSize){
      val nextTuple = result(resIndex)
      resIndex +=1
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
