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

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(name = "DataToSketch", value = "_FUNC_(value, k) - "
    + "Returns an ItemsSketch<String> in a serialized form as a binary blob."
    + " Values must be of string type."
    + " Parameter k controls the accuracy and the size of the sketch."
    + " If k is ommitted, the default is used.")
@SuppressWarnings("javadoc")
public class DataToStringsSketchUDAF extends DataToItemsSketchUDAF<String> {

  @Override
  public GenericUDAFEvaluator createEvaluator() {
    return new DataToStringsSketchEvaluator();
  }

  static class DataToStringsSketchEvaluator extends DataToSketchEvaluator<String> {

    DataToStringsSketchEvaluator() {
      super(Comparator.naturalOrder(), new ArrayOfStringsSerDe());
    }

    @Override
    public String extractValue(final Object data, final ObjectInspector objectInspector)
        throws HiveException {
      final Object value = inputObjectInspector.getPrimitiveJavaObject(data);
      if (value instanceof String) {
        return (String) value;
      } else if (value instanceof HiveChar) {
        return ((HiveChar) value).getValue();
      } else if (value instanceof HiveVarchar) {
        return ((HiveVarchar) value).getValue();
      } else {
        throw new UDFArgumentTypeException(0, "unsupported type " + value.getClass().getName());
      }
    }

  }

}
