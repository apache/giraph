package org.apache.giraph.examples.feature_diffusion_utils.datastructures;



import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class LabelingVertexValue implements Writable {


    protected long label;
    protected int treshold;
    protected boolean changed = false;
    protected double temp;
    protected HashMap<Long, Long> neighboorsLabels=new HashMap<Long, Long>();

    public LabelingVertexValue(){
        this.treshold=1;
    }

    public LabelingVertexValue(int treshold){
        this.treshold=treshold;
    }

    public void readFields(DataInput in) throws IOException {
        label=in.readLong();
        treshold=in.readInt();
        changed=in.readBoolean();
        temp=in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(label);
        out.writeInt(treshold);
        out.writeBoolean(changed);
        out.writeDouble(temp);
    }

    public Long getLabel() {return label;}
    
    public HashMap<Long, Long> getNeighboorsLabel() {return neighboorsLabels;}

    public boolean isChanged() {return changed;}

    public void setLabel(long label) {
        this.label=label;
        this.changed=true;
    }

    public void setChanged(boolean newChanged) {
        this.changed = newChanged;
    }


    public void updateNeighboorLabel(long id,long label) {
        if(!neighboorsLabels.containsKey(id))
        	neighboorsLabels.put(id, label);
        else
        if(neighboorsLabels.get(id) > label)
        	neighboorsLabels.put(id, label);
    }
    
    public double getTemp() {
    	return temp;
    }

    public void setTemp(double temp) {
    	this.temp=temp;
    }
    public String toString() {
        return ""+label+","+treshold;
    }


}
