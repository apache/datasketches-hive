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

package org.apache.datasketches.hive.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

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
            .copyBytes());
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
