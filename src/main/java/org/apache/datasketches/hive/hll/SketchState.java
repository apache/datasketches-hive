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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

class SketchState extends State {

  private HllSketch sketch_;

  @Override
  boolean isInitialized() {
    return this.sketch_ != null;
  }

  @Override
  void init(final int logK, final TgtHllType type) {
    super.init(logK, type);
    this.sketch_ = new HllSketch(logK, type);
  }

  @Override
  void update(final Object data, final PrimitiveObjectInspector objectInspector) {
    switch (objectInspector.getPrimitiveCategory()) {
      case BINARY:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getBinary(data, objectInspector)
            .copyBytes());
        return;
      case BYTE:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getByte(data, objectInspector));
        return;
      case DOUBLE:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getDouble(data, objectInspector));
        return;
      case FLOAT:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getFloat(data, objectInspector));
        return;
      case INT:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getInt(data, objectInspector));
        return;
      case LONG:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getLong(data, objectInspector));
        return;
      case STRING:
        // This gets the java String and hashes the underlying UTF-16 array. It was an early
        //attempt at optimization, which unfortunately is different from all other String
        //hashing in the library which assume UTF-8. This can't be changed due to all the
        //history users have created using this current method. Users that don't want the
        //string conversion here can either convert their strings to the underlying UTF-16
        //and use case CHAR, or if their strings are already UTF-8, grab the underlying
        //byte array and use the BINARY method.
        this.sketch_.update(PrimitiveObjectInspectorUtils.getString(data, objectInspector)
            .toCharArray());
        return;
      case CHAR:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getHiveChar(data, objectInspector)
            .getValue().toCharArray());
        return;
      case VARCHAR:
        this.sketch_.update(PrimitiveObjectInspectorUtils.getHiveVarchar(data, objectInspector)
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
    if (this.sketch_ == null) { return null; }
    return this.sketch_;
  }

  @Override
  void reset() {
    this.sketch_ = null;
  }

}
