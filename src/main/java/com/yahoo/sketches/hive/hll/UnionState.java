/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

class UnionState extends State {

  private Union union_;

  boolean isInitialized() {
    return union_ != null;
  }

  @Override
  void init(final int lgK, final TgtHllType type) {
    super.init(lgK, type);
    union_ = new Union(lgK);
  }

  void update(final HllSketch sketch) {
    union_.update(sketch);
  }

  @Override
  HllSketch getResult() {
    if (union_ == null) { return null; }
    return union_.getResult(this.getType());
  }

  @Override
  void reset() {
    union_ = null;
  }

}
