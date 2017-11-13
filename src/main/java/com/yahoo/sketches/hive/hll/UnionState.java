/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

class UnionState extends State {

  private Union union_;

  @Override
  boolean isInitialized() {
    return union_ != null;
  }

  @Override
  void init(final int lgK, final TgtHllType type) {
    super.init(lgK, type);
    union_ = new Union(lgK);
  }

  @Override
  void update(final Object data, final PrimitiveObjectInspector objectInspector) {
    switch (objectInspector.getPrimitiveCategory()) {
      case BINARY:
        union_.update(PrimitiveObjectInspectorUtils.getBinary(data, objectInspector)
            .getBytes());
        return;
      case BYTE:
        union_.update(PrimitiveObjectInspectorUtils.getByte(data, objectInspector));
        return;
      case DOUBLE:
        union_.update(PrimitiveObjectInspectorUtils.getDouble(data, objectInspector));
        return;
      case FLOAT:
        union_.update(PrimitiveObjectInspectorUtils.getFloat(data, objectInspector));
        return;
      case INT:
        union_.update(PrimitiveObjectInspectorUtils.getInt(data, objectInspector));
        return;
      case LONG:
        union_.update(PrimitiveObjectInspectorUtils.getLong(data, objectInspector));
        return;
      case STRING:
        // conversion to char[] avoids costly UTF-8 encoding
        union_.update(PrimitiveObjectInspectorUtils.getString(data, objectInspector)
            .toCharArray());
        return;
      case CHAR:
        union_.update(PrimitiveObjectInspectorUtils.getHiveChar(data, objectInspector)
            .getValue().toCharArray());
        return;
      case VARCHAR:
        union_.update(PrimitiveObjectInspectorUtils.getHiveVarchar(data, objectInspector)
            .getValue().toCharArray());
        return;
      default:
        throw new IllegalArgumentException(
          "Unrecongnized input data type " + data.getClass().getSimpleName() + " category "
          + objectInspector.getPrimitiveCategory() + ", please use data of the following types: "
          + "byte, double, float, int, long, char, varchar or string.");
    }
  }

  void update(final HllSketch sketch) {
    union_.update(sketch);
  }

  @Override
  HllSketch getResult() {
    if (union_ == null) { return null; }
    return union_.getResult(getType());
  }

  @Override
  void reset() {
    union_ = null;
  }

}
