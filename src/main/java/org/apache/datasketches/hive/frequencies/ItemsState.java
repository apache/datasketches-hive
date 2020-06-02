/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.frequencies;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

class ItemsState<T> extends AbstractAggregationBuffer {

  private int maxMapSize_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private ItemsSketch<T> sketch;

  ItemsState(final ArrayOfItemsSerDe<T> serDe) {
    serDe_ = serDe;
  }

  // initializing maxMapSize is needed for building sketches using update(value)
  // not needed for merging sketches using update(sketch)
  void init(final int maxMapSize) {
    maxMapSize_ = maxMapSize;
    sketch = new ItemsSketch<>(maxMapSize_);
  }

  boolean isInitialized() {
    return sketch != null;
  }

  void update(final T value) {
    sketch.update(value);
  }

  void update(final Memory serializedSketch) {
    final ItemsSketch<T> incomingSketch = ItemsSketch.getInstance(serializedSketch, serDe_);
    if (sketch == null) {
      sketch = incomingSketch;
    } else {
      sketch.merge(incomingSketch);
    }
  }

  public ItemsSketch<T> getResult() {
    return sketch;
  }

  void reset() {
    sketch = null;
  }

}
