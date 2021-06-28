package org.apache.giraph.examples.feature_diffusion.datastructures;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class LabelingVertexValue implements Writable {

  protected long label;
  protected int threshold;
  protected boolean labelJustChanged = false;
  protected double temporaryValue;
  protected HashMap<Long, Long> neighborsLabels = new HashMap<Long, Long>();

  public LabelingVertexValue() {
    this.threshold = 1;
  }

  public LabelingVertexValue(int threshold) {
    this.threshold = threshold;
  }

  public void readFields(DataInput in) throws IOException {
    label = in.readLong();
    threshold = in.readInt();
    labelJustChanged = in.readBoolean();
    temporaryValue = in.readDouble();
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(label);
    out.writeInt(threshold);
    out.writeBoolean(labelJustChanged);
    out.writeDouble(temporaryValue);
  }

  public long getLabel() {
    return label;
  }

  public HashMap<Long, Long> getNeighborsLabel() {
    return neighborsLabels;
  }

  public boolean isChanged() {
    return labelJustChanged;
  }

  public void setLabel(long label) {
    this.label = label;
    this.labelJustChanged = true;
  }

  public void setChanged(boolean newChanged) {
    this.labelJustChanged = newChanged;
  }

  public void updateNeighboorLabel(long id, long label) {
    if (!neighborsLabels.containsKey(id)) neighborsLabels.put(id, label);
    else if (neighborsLabels.get(id) > label) neighborsLabels.put(id, label);
  }

  public double getTemp() {
    return temporaryValue;
  }

  public void setTemp(double temp) {
    this.temporaryValue = temp;
  }

  public String toString() {
    return "" + label + "," + threshold;
  }
}
