package com.yahoo.sketches.hive.quantiles;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.QuantilesSketch;

@Description(name = "GetQuantiles", value = "_FUNC_(sketch, fractions...) -"
  + " Returns quantile values from a QuantileSketch given a list of fractions."
  + " The fractions represent normalized ranks and must be from 0 to 1 inclusive."
  + " For example, the fraction of 0.5 corresponds to 50th percentile,"
  + " which is the median value of the distribution (the number separating the higher"
  + " half of the probability distribution from the lower half).")
public class GetQuantiles extends UDF {

  public List<Double> evaluate(final BytesWritable serializedSketch, final Double... fractions) {
    if (serializedSketch == null) return null;
    final QuantilesSketch sketch = QuantilesSketch.heapify(new NativeMemory(serializedSketch.getBytes()));
    return primitivesToList(sketch.getQuantiles(objectsToPrimitives(fractions)));
  }

  private static double[] objectsToPrimitives(final Double[] array) {
    final double[] result = new double[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i];
    }
    return result;
  }

  private static List<Double> primitivesToList(final double[] array) {
    final List<Double> result = new ArrayList<Double>(array.length);
    for (double item: array) result.add(item);
    return result;
  }

}
