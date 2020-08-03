package org.apache.flink.table.api.example.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}

object ScalaStreamWordCount {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val data = Seq("Flink", "Bob", "Bob", "something", "Hello", "Flink", "Bob")
    // 最简单的获取Source方式
    val source = env.fromCollection(data).toTable(tEnv, 'word)

    // 单词统计核心逻辑
    val result = source
      .groupBy('word) // 单词分组
      .select('word, 'word.count) // 单词统计

    result.toRetractStream[(String, Long)].print()
    env.execute()
  }
}

