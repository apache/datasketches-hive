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

package org.apache.datasketches.hive.quantiles;

import java.util.Comparator;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

class ItemsUnionState<T> extends AbstractAggregationBuffer {

  private final Comparator<? super T> comparator_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private ItemsUnion<T> union;

  ItemsUnionState(final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    this.comparator_ = comparator;
    this.serDe_ = serDe;
  }

  // initializing is needed only in the first phase (iterate)
  void init(final int k) {
    if (k > 0) {
      this.union = ItemsUnion.getInstance(k, this.comparator_);
    } else {
      this.union = ItemsUnion.getInstance(this.comparator_);
    }
  }

  boolean isInitialized() {
    return this.union != null;
  }

  void update(final T value) {
    if (this.union == null) {
      this.union = ItemsUnion.getInstance(this.comparator_);
    }
    this.union.update(value);
  }

  void update(final Memory serializedSketch) {
    final ItemsSketch<T> incomingSketch =
        ItemsSketch.getInstance(serializedSketch, this.comparator_, this.serDe_);
    if (this.union == null) {
      this.union = ItemsUnion.getInstance(incomingSketch);
    } else {
      this.union.update(incomingSketch);
    }
  }

  public ItemsSketch<T> getResult() {
    if (this.union == null) { return null; }
    return this.union.getResultAndReset();
  }

  void reset() {
    this.union = null;
  }

}
