/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;

class ArrayOfDoublesUnionState extends ArrayOfDoublesState {

  private ArrayOfDoublesUnion union_;

  boolean isInitialized() {
    return union_ != null;
  }

  void init(final int numNominalEntries, final int numValues) {
    super.init(numNominalEntries, numValues);
    union_ = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(numNominalEntries).setNumberOfValues(numValues).buildUnion();
  }

  void update(final ArrayOfDoublesSketch sketch) {
    union_.update(sketch);
  }

  @Override
  ArrayOfDoublesSketch getResult() {
    if (union_ == null) return null;
    return union_.getResult();
  }

  @Override
  void reset() {
    union_ = null;    
  }

}
