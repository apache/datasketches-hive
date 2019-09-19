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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * This is a generic implementation to be specialized in subclasses
 * @param <T> type of item
 */
@SuppressWarnings("javadoc")
public abstract class DataToItemsSketchUDAF<T> extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info)
      throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length != 2) {
      throw new UDFArgumentException("Two arguments expected");
    }

    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
          + inspectors[0].getTypeName() + " was recieved");
    }

    if (inspectors[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
          + inspectors[1].getTypeName() + " was recieved");
    }
    final PrimitiveObjectInspector inspector2 = (PrimitiveObjectInspector) inspectors[1];
    if (inspector2.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentTypeException(0, "Integer value expected as the second argument, but "
          + inspector2.getPrimitiveCategory().name() + " was received");
    }

    return createEvaluator();
  }

  public abstract GenericUDAFEvaluator createEvaluator();

  public static abstract class DataToItemsSketchEvaluator<T> extends ItemsEvaluator<T> {

    private PrimitiveObjectInspector maxMapSizeObjectInspector;

    public DataToItemsSketchEvaluator(final ArrayOfItemsSerDe<T> serDe) {
      super(serDe);
    }

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters)
        throws HiveException {
      final ObjectInspector result = super.init(mode, parameters);

      // Parameters:
      // In PARTIAL1 and COMPLETE mode, the parameters are original data.
      // In PARTIAL2 and FINAL mode, the parameters are just partial aggregations.
      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        if (parameters.length > 1) {
          maxMapSizeObjectInspector = (PrimitiveObjectInspector) parameters[1];
        }
      }

      return result;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void iterate(final AggregationBuffer buf, final Object[] data) throws HiveException {
      if (data[0] == null) { return; }
      @SuppressWarnings("unchecked")
      final ItemsState<T> state = (ItemsState<T>) buf;
      if (!state.isInitialized()) {
        final int maxMapSize = PrimitiveObjectInspectorUtils.getInt(data[1], maxMapSizeObjectInspector);
        state.init(maxMapSize);
      }
      state.update(extractValue(data[0], inputObjectInspector));
    }

    public abstract T extractValue(final Object data, final ObjectInspector objectInspector)
        throws HiveException;

  }

}
