package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.QuantilesSketch;

@Description(name = "GetQuantile", value = "_FUNC_(sketch, fraction) -"
  + " Returns a quantile value from a QuantilesSketch. A single value for a"
  + " given fraction is returned. The fraction represents a normalized rank, and must be"
  + " from 0 to 1 inclusive. For example, the fraction of 0.5 corresponds to 50th percentile,"
  + " which is the median value of the distribution (the number separating the higher half"
  + " of the probability distribution from the lower half).")
public class GetQuantile extends UDF {

  public Double evaluate(final BytesWritable serializedSketch, final double fraction) {
    if (serializedSketch == null) return null;
    final QuantilesSketch sketch = QuantilesSketch.heapify(new NativeMemory(serializedSketch.getBytes()));
    return sketch.getQuantile(fraction);
  }

}
