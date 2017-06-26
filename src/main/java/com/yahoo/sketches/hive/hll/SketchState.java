/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;

class SketchState extends State {

  private HllSketch sketch_;

  boolean isInitialized() {
    return sketch_ != null;
  }

  @Override
  void init(final int logK, final TgtHllType type) {
    super.init(logK, type);
    sketch_ = new HllSketch(logK, type);
  }

  void update(final Object data, final PrimitiveObjectInspector keyObjectInspector) {
    switch (keyObjectInspector.getPrimitiveCategory()) {
    case BINARY:
      sketch_.update(PrimitiveObjectInspectorUtils.getBinary(data, keyObjectInspector).getBytes());
      return;
    case BYTE:
      sketch_.update(PrimitiveObjectInspectorUtils.getByte(data, keyObjectInspector));
      return;
    case DOUBLE:
      sketch_.update(PrimitiveObjectInspectorUtils.getDouble(data, keyObjectInspector));
      return;
    case FLOAT:
      sketch_.update(PrimitiveObjectInspectorUtils.getFloat(data, keyObjectInspector));
      return;
    case INT:
      sketch_.update(PrimitiveObjectInspectorUtils.getInt(data, keyObjectInspector));
      return;
    case LONG:
      sketch_.update(PrimitiveObjectInspectorUtils.getLong(data, keyObjectInspector));
      return;
    case STRING:
      sketch_.update(PrimitiveObjectInspectorUtils.getString(data, keyObjectInspector));
      return;
    default:
      throw new IllegalArgumentException(
          "Unrecongnized input data type, please use data of type: "
      + "byte, double, float, int, long, or string only.");
    }
  }

  @Override
  HllSketch getResult() {
    if (sketch_ == null) { return null; }
    return sketch_;
  }

  @Override
  void reset() {
    sketch_ = null;
  }

}
