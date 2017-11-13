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

  @Override
  boolean isInitialized() {
    return sketch_ != null;
  }

  @Override
  void init(final int logK, final TgtHllType type) {
    super.init(logK, type);
    sketch_ = new HllSketch(logK, type);
  }

  @Override
  void update(final Object data, final PrimitiveObjectInspector objectInspector) {
    switch (objectInspector.getPrimitiveCategory()) {
      case BINARY:
        sketch_.update(PrimitiveObjectInspectorUtils.getBinary(data, objectInspector)
            .getBytes());
        return;
      case BYTE:
        sketch_.update(PrimitiveObjectInspectorUtils.getByte(data, objectInspector));
        return;
      case DOUBLE:
        sketch_.update(PrimitiveObjectInspectorUtils.getDouble(data, objectInspector));
        return;
      case FLOAT:
        sketch_.update(PrimitiveObjectInspectorUtils.getFloat(data, objectInspector));
        return;
      case INT:
        sketch_.update(PrimitiveObjectInspectorUtils.getInt(data, objectInspector));
        return;
      case LONG:
        sketch_.update(PrimitiveObjectInspectorUtils.getLong(data, objectInspector));
        return;
      case STRING:
        // conversion to char[] avoids costly UTF-8 encoding
        sketch_.update(PrimitiveObjectInspectorUtils.getString(data, objectInspector)
            .toCharArray());
        return;
      case CHAR:
        sketch_.update(PrimitiveObjectInspectorUtils.getHiveChar(data, objectInspector)
            .getValue().toCharArray());
        return;
      case VARCHAR:
        sketch_.update(PrimitiveObjectInspectorUtils.getHiveVarchar(data, objectInspector)
            .getValue().toCharArray());
        return;
      default:
        throw new IllegalArgumentException(
          "Unrecongnized input data type " + data.getClass().getSimpleName() + " category "
          + objectInspector.getPrimitiveCategory() + ", please use data of the following types: "
          + "byte, double, float, int, long, char, varchar or string.");
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
