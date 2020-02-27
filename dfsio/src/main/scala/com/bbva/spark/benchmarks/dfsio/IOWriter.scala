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

import java.io._
import java.security.{DigestInputStream, MessageDigest}
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class IOWriter(hadoopConf: Configuration, dataDir: String) extends IOTestBase(hadoopConf, dataDir) {

  def doIO(fileName: String, fileSize: BytesSize)(implicit conf: Configuration, fs: FileSystem): (BytesSize, Latency) = {

    val bufferSize = conf.getInt("test.io.file.buffer.size", DefaultBufferSize) // TODO GET RID OF DEFAULT
    val validateData = conf.getBoolean("dce.write.validate-data.enabled", false) // TODO GET RID OF DEFAULT

    val buffer: Array[Byte] = Array.tabulate[Byte](bufferSize)(i => ('0' + i % 50).toByte)
    val filePath = new Path(dataDir, fileName.toString)

    val localFilePath = new Path(s"/fluir_data/${fileName.toString}-${UUID.randomUUID().toString}")
    val localFile = new File(localFilePath.toString)

    val localOutStream = if (validateData) {
      logger.info("Creating file {} with size {}", localFilePath.toString, fileSize.toString)
      Some(new BufferedOutputStream( new FileOutputStream(localFile)))
    } else { None }

    logger.info("Creating file {} with size {}", filePath.toString, fileSize.toString)

    val out = fs.create(filePath, true, bufferSize)
    var latency : Double = 0
    var minLatency: Double = Double.MaxValue
    var maxLatency: Double = Double.MinValue
    var numWrites: Long = 0

    try {
      for (remaining <- fileSize to 0 by -bufferSize) {
        val currentSize = if (bufferSize.toLong < remaining) bufferSize else remaining.toInt
        val startTime: Long = System.nanoTime()
        out.write(buffer, 0, currentSize)
        if (localOutStream.nonEmpty) localOutStream.get.write(buffer, 0, currentSize)
        val currLatency: Double = (System.nanoTime() - startTime).toDouble/1000
        latency += currLatency
        if (currLatency < minLatency) minLatency = currLatency
        else if (currLatency > maxLatency) maxLatency = currLatency
        numWrites += 1
      }
    } finally {
      out.close()
      if (localOutStream.nonEmpty) localOutStream.get.close()
    }

    logger.info("File {} created with size {}", fileName, fileSize.toString)
    if (validateData) {
      logger.info("File {} created with size {}", localFilePath.toString, fileSize.toString)

      // validate data
      val localFileHash = computeHash(localFilePath)
      val hdfsFileHash = computeHash(filePath, Some(fs))
      if (localFileHash.equals(hdfsFileHash)) {
        logger.info(s"MD5Hash of local file: ${localFilePath.toString}" +
          s" is same as hdfs file: ${filePath.toString}")
        localFile.delete()
      } else {
        throw new RuntimeException(s"MD5Hash of local file: ${localFilePath.toString}" +
          s" doesn't match with hdfs file: ${filePath.toString}")
      }
    }

    (fileSize, Latency(total = latency, blocks = numWrites, min = minLatency, max = maxLatency))
  }

  // Compute a hash of a file
  def computeHash(filePath: Path, fs: Option[FileSystem] = None): String = {
    val buffer = new Array[Byte](8192)
    val md5 = MessageDigest.getInstance("MD5")

    val in = if (fs.nonEmpty) {
      fs.get.open(filePath)
    } else {
      new FileInputStream(new File(filePath.toString))
    }

    val dis = new DigestInputStream(in, md5)
    try { while (dis.read(buffer) != -1) { } } finally { dis.close() }

    md5.digest.map("%02x".format(_)).mkString
  }
}
