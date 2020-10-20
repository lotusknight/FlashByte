/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// add by rl

package org.apache.spark.storage

import java.nio.ByteBuffer
import java.nio.DoubleBuffer
import java.util.LinkedHashMap

// import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.util.collection.NativeBufferVector

private[storage] class TransForKMeans {


  var _size = 0
  var stock_values = new NativeBufferVector[Double](64)
  var info_array = new NativeBufferVector[Int](64)

  def transfer(trans: Array[Array[Double]]): Unit = {
    // todo
   // val trans = array.map(vector => vector.toArray) // .asInstanceOf[Array[Array[Double]]]
    info_array += trans.length
    var arr_ind = 0 // index for out loop
    while (trans.length != arr_ind) {
      info_array += trans(arr_ind).length
      for (w <- 0 to trans(arr_ind).length - 1) {
        stock_values += trans(arr_ind)(w)
      }
      arr_ind += 1
    }
  }

  // return idx for current storage (idx, fst_bb, snd_bb)
  def allocateNative: (Integer, Long) = {
    stock_values.trim()
    _size = stock_values.size * 8
    var fst_bb = ByteBuffer.allocateDirect(_size)
    for (w <- 0 until stock_values.size-1) {
      fst_bb.putDouble(stock_values.toArray(w)) // (for putDouble) is toArray wasteful?
    }
    fst_bb.rewind()
    stock_values = null
    TransForKMeans.entries.synchronized{
      TransForKMeans.entries.put(TransForKMeans.index, fst_bb)
      TransForKMeans.index += 1
    }

    (TransForKMeans.index-1, _size)
  }

  def transback(idx: Integer): Array[Array[Double]] = {
    val entry = TransForKMeans.entries.synchronized{
      TransForKMeans.entries.get(idx)}
    entry match {
      case null =>
        throw new IllegalArgumentException("there is no such block in native")
      case fst_bb: ByteBuffer =>
        transback(fst_bb.asDoubleBuffer())
    }

  }

  def transback(doubleBuffer: DoubleBuffer): Array[Array[Double]] = {

    val result = new Array[Array[Double]](info_array.toArray(0))

    var position: Integer = 0
    for (idx <- 0 to info_array.toArray(0) - 1) {
      // result(idx) = useIndexArray(doubleBuffer, info_array.toArray(idx + 1), position)
      val length = info_array.toArray(idx + 1)
      result(idx) = new Array[Double](length)
      for (idx2 <- 0 to length-1) {
        result(idx)(idx2) = doubleBuffer.get(position)
      //  printf("%f, ", result(idx)(idx2))
        position += 1
      }

    }
    result
  }

  // todo: how to remove a bytebuffer?
  def remove(idx: Integer): Unit = {
    val entry = TransForKMeans.entries.synchronized {
      TransForKMeans.entries.remove(idx)
    }
  }

}

object TransForKMeans {

  private val entries = new LinkedHashMap[Integer, ByteBuffer](32, 0.75f, true)
  private var index: Integer = 0

}

