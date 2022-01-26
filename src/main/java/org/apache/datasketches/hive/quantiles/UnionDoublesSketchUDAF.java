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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(name = "Union", value = "_FUNC_(sketch) - "
    + "Returns a QuantilesSketch in a serialized form as a binary blob."
    + " Input values are also serialized sketches.")
@SuppressWarnings("deprecation")
public class UnionDoublesSketchUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if ((inspectors.length != 1) && (inspectors.length != 2)) {
      throw new UDFArgumentException("One or two arguments expected");
    }
    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[0], 0, PrimitiveCategory.BINARY);
    if (inspectors.length == 2) {
      ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[1], 1, PrimitiveCategory.INT);
    }
    return new UnionEvaluator();
  }

  static class UnionEvaluator extends DoublesEvaluator {

    @Override
    public void iterate(final AggregationBuffer buf, final Object[] data) throws HiveException {
      if (data[0] == null) { return; }
      final DoublesUnionState state = (DoublesUnionState) buf;
      if (!state.isInitialized()) {
        int k = 0;
        if (this.kObjectInspector != null) {
          k = PrimitiveObjectInspectorUtils.getInt(data[1], this.kObjectInspector);
        }
        state.init(k);
      }
      merge(buf, data[0]);
    }

  }

}
