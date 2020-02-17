/*
 * Copyright 2017 Banco Bilbao Vizcaya Argentaria S.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bbva.spark.benchmarks.dfsio

import org.apache.spark.rdd.RDD

case class Latency(total: Double, blocks: BytesSize, min: Double, max: Double)
case class Stats(tasks: Long, size: BytesSize, time: Long, rate: Float, sqRate: Float, latency: Latency)

object StatsAccumulator {

  def accumulate(rdd: RDD[Stats]): Stats = rdd.reduce {
    (s1, s2) => s1.copy(
      tasks = s1.tasks + s2.tasks,
      size = s1.size + s2.size,
      time = s1.time + s2.time,
      rate = s1.rate + s2.rate,
      latency = Latency(total = s1.latency.total + s2.latency.total, blocks = s1.latency.blocks + s2.latency.blocks, min = if (s1.latency.min < s2.latency.min) s1.latency.min else s2.latency.min, max = if (s1.latency.max > s2.latency.max) s1.latency.max else s2.latency.max),
      sqRate = s1.sqRate + s2.sqRate
    )
  }
}
