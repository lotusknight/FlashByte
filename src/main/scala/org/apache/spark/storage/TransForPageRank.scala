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
import java.util.LinkedHashMap

import org.apache.spark.util.collection.NativeBufferVector

private[storage] class TransForPageRank {


  var _size = 0
  var stock_index = new NativeBufferVector[Byte](64)
  var stock_values = new NativeBufferVector[Byte](64)
  var fst_idx = 0
  var snd_idx = 0
  // 1st is the length of Array. 2nd to the end are lengths of sub-Array
  // we can use this metadata to help get the array back from our native
  // storage.
  var info_array = new NativeBufferVector[Int](64)

  val symbol_dou = ','.asInstanceOf[Byte]
  val symbol_ju = '.'.asInstanceOf[Byte]

  // these 2 are used for us to index where the pointers are in the byte Array
  var transback_idx1 = 0
  var transback_idx2 = 0

  def transfer(array: Array[Tuple2[String, Array[String]]]): Unit = {
    info_array += array.length
    var arr_ind = 0 // index for out loop
    while (array.length != arr_ind) {
      info_array += array(arr_ind)._2.length
      var tmp = array(arr_ind)._1.getBytes()
      for (w <- 0 to tmp.length - 1) {
        stock_index += tmp(w)
      }
      stock_index += symbol_dou
      var sub_ind = 0 // index for inner loop
      while (array(arr_ind)._2.length != sub_ind) {
        var tmp = array(arr_ind)._2(sub_ind).getBytes()
        // println("the length of string is " + tmp.length +":"+ Arrays.toString(tmp))
        for (w <- 0 to tmp.length - 1) {
          stock_values += tmp(w)
        }
        sub_ind += 1
        if (array(arr_ind)._2.length != sub_ind) {
          stock_values += symbol_dou
        }
      }
      stock_values += symbol_ju
      arr_ind += 1
    }
  }

  // return idx for current storage (idx, fst_bb, snd_bb)
  def allocateNative: (Long, Long) = {
    stock_index.trim() // do I need this?
    stock_values.trim()
    _size = stock_index.size + stock_values.size // Is this the true size?
    var fst_bb = ByteBuffer.allocateDirect(stock_index.size)
    var snd_bb = ByteBuffer.allocateDirect(stock_values.size)
    fst_bb = ByteBuffer.wrap(stock_index.toArray)
    snd_bb = ByteBuffer.wrap(stock_values.toArray)
    stock_index = null
    stock_values = null
//    println("1st Direct ByteBuffer: " + Arrays.toString(fst_bb.array()))
//    println("2st Direct ByteBuffer: " + Arrays.toString(snd_bb.array()))

  //  I need a hash map to store the storage with a idx
  //  need to clean the stock_values and stock_index

    TransForPageRank.entries.synchronized{
      TransForPageRank.entries.put(TransForPageRank.index, (fst_bb, snd_bb))
      TransForPageRank.index += 1
    }

    (TransForPageRank.index, _size)
  }

  def transback(idx: Long): Array[Tuple2[String, Array[String]]] = {
    val entry = TransForPageRank.entries.synchronized{
      TransForPageRank.entries.get(TransForPageRank.index)}
    entry match {
      case null =>
        throw new IllegalArgumentException("there is no such block in native")
      case (fst_bb: ByteBuffer, snd_bb: ByteBuffer) =>
        transback(fst_bb.array(), snd_bb.array())
    }

  }

/*
  def transback(idx: Int): Option[Array[Tuple2[String, Array[String]]]] = {
    val entry = TransForPageRank.entries.synchronized{
      TransForPageRank.entries.get(TransForPageRank.index)}
    entry match {
      case null =>
        throw new IllegalArgumentException("there is no such block in native")
      case (fst_bb: ByteBuffer, snd_bb: ByteBuffer) =>
        Some(transback(fst_bb.array(), snd_bb.array()))
    }

  }
*/

  def transback(fstArray: Array[Byte],
                sndArray: Array[Byte]): Array[Tuple2[String, Array[String]]] = {

    val result = new Array[Tuple2[String, Array[String]]](info_array.toArray(0))

    for (idx <- 0 to info_array.toArray(0) - 1) {
      result(idx) = Tuple2(useIndex1stArray(fstArray),
        useIndex2ndArray(sndArray, info_array.toArray(idx + 1)))
    }
    result
  }

  // todo: how to remove a bytebuffer?
  def remove(idx: Long): Unit = {
    val entry = TransForPageRank.entries.synchronized {
      TransForPageRank.entries.remove(idx)
    }
/*
    if (entry != null) {
      entry match {
        case (fst: ByteBuffer, snd: ByteBuffer) =>
          fst = null
          snd = null
        case _ =>
      }
    }
*/
  }

  def useIndex1stArray(array: Array[Byte]): String = {
    // "123"
    var tmp = ""
    while (array(transback_idx1) != symbol_dou) {
      // tmp += array(transback_idx1)
      // need to transfer to integer, or each will be divided to 2 bytes
      tmp += (array(transback_idx1) - 48)
      transback_idx1 += 1
    }
    // if (transback_idx1 != info_array.toArray(0)-1) {transback_idx1 += 1}
    transback_idx1 += 1 // if that's the end we won't access again so it won't overflow
    tmp
  }

  def useIndex2ndArray(array: Array[Byte], length: Int): Array[String] = {
    // Array("456","789")
    var sub_array = new Array[String](length)
    for (idx <- 0 to length - 1) {
      var tmp = "" // if I don't use this tmp, every elements will have a null before that
      while (array(transback_idx2) != symbol_dou && array(transback_idx2) != symbol_ju) {
        // sub_array(idx) += array(transback_idx2) // this is the wrong one
        tmp += (array(transback_idx2) - 48)
        // need to transfer to integer, or each will be divided to 2 byte
        // tmp += array(transback_idx2)
        // printf("%c,",array(transback_idx2))
        transback_idx2 += 1
      }
      //      println(tmp)
      sub_array(idx) = tmp
      transback_idx2 += 1
    }
    //    if(array(transback_idx2-1) != symbol_ju) println("Warning.wrong input!")
    sub_array
  }

}

object TransForPageRank {

  private val entries = new LinkedHashMap[Int, (ByteBuffer, ByteBuffer)](32, 0.75f, true)
  private var index = 0

}