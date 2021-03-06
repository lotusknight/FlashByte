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

package org.apache.spark.util.collection

import scala.reflect.ClassTag

private[spark] class NativeBufferVector[V: ClassTag](initialSize: Int = 64) {

  private var _numElements = 0
  private var _array: Array[V] = _

  _array = new Array[V](initialSize)

  def +=(value: V): Unit = {
    if (_numElements == _array.length) {
      resize(_array.length * 2)
    }
    _array(_numElements) = value
    _numElements += 1
  }

  def capacity: Int = _array.length

  def length: Int = _numElements

  def size: Int = _numElements

  def iterator: Iterator[V] = new Iterator[V] {
    var index = 0
    override def hasNext: Boolean = index < _numElements
    override def next(): V = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val value = _array(index)
      index += 1
      value
    }
  }

  def array: Array[V] = _array

  def trim(): NativeBufferVector[V] = resize(size)

  def resize(newLength: Int): NativeBufferVector[V] = {
    _array = copyArrayWithLength(newLength)
    if (newLength < _numElements) {
      _numElements = newLength
    }
    this
  }

  def toArray: Array[V] = {
    copyArrayWithLength(size)
  }

  private def copyArrayWithLength(length: Int): Array[V] = {
    val copy = new Array[V](length)
    _array.copyToArray(copy)
    copy
  }
}
