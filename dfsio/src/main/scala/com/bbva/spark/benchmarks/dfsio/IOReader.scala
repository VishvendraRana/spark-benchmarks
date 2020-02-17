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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class IOReader(hadoopConf: Configuration, dataDir: String) extends IOTestBase(hadoopConf, dataDir) {

  def doIO(fileName: String, fileSize: BytesSize)(implicit conf: Configuration, fs: FileSystem): (BytesSize, Latency) = {

    val bufferSize = conf.getInt("test.io.file.buffer.size", DefaultBufferSize) // TODO GET RID OF DEFAULT
    val buffer: Array[Byte] = new Array[Byte](bufferSize)
    val filePath = new Path(dataDir, fileName.toString)

    logger.error("Reading file {} with size {}", filePath.toString, fileSize.toString)
    var latency : Double = 0
    var minLatency: Double = Double.MaxValue
    var maxLatency: Double = Double.MinValue
    var numReads: Long = 0

    val in = fs.open(filePath)

    var actualSize: Long = 0 // TODO improve this
    try {

      def read: (Int, Double) = {
        val startTime: Long = System.nanoTime()
        val currentSize = in.read(buffer, 0, bufferSize)
        val currLatency: Double = (System.nanoTime() - startTime).toDouble/1000
        (currentSize, currLatency)
      }

      Stream.continually(read)
        .takeWhile(_._1 > 0 && actualSize < fileSize)
        .foreach { t =>
          actualSize += t._1

          latency += t._2
          if (t._2 < minLatency) minLatency = t._2 else if (t._2 > maxLatency) maxLatency = t._2
          numReads += 1
          logger.debug(s"Reading chunk of size ${t._1}. Currently: $actualSize / $fileSize")
        }
    } finally {
      in.close()
    }

    logger.info("File {} with size {} read successfully", fileName, actualSize.toString)

    (actualSize, Latency(total = latency, blocks = numReads, min = minLatency, max = maxLatency))
  }

}