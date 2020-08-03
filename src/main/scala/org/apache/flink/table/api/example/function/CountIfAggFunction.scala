package org.apache.flink.table.api.example.function

import java.lang.{Iterable => JIterable, Long => JLong}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for count aggregate function */
class CountAccumulator extends JTuple1[Long] {
  f0 = 0L //count
  override def copy[T <: Tuple](): T = ???
}

/**
 * built-in count aggregate function
 */
class CountIfAggFunction extends AggregateFunction[JLong, CountAccumulator] {


  // process argument is optimized by Calcite.
  // For instance count(42) or count(*) will be optimized to count().
  def retract(acc: CountAccumulator): Unit = {
    acc.f0 -= 1L
  }

  def accumulate(acc: CountAccumulator, value: Boolean): Unit = {
    if (value) {
      acc.f0 += 1L
    }
  }

  def retract(acc: CountAccumulator, value: Boolean): Unit = {
    if (value) {
      acc.f0 -= 1L
    }
  }

  override def getValue(acc: CountAccumulator): JLong = {
    acc.f0
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }

  def resetAccumulator(acc: CountAccumulator): Unit = {
    acc.f0 = 0L
  }

  override def getAccumulatorType: TypeInformation[CountAccumulator] = {
    new TupleTypeInfo(classOf[CountAccumulator], BasicTypeInfo.LONG_TYPE_INFO)
  }

  override def getResultType: TypeInformation[JLong] =
    BasicTypeInfo.LONG_TYPE_INFO
}
