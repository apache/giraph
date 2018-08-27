package org.apache.giraph.examples.feature_diffusion_utils.datastructures;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
// DEBUG
//import java.util.LinkedList;
import java.util.LinkedList;

public class DiffusionVertexValue implements Writable {

    protected int vertexThreshold;
    protected int label;
    protected double currentActivationProbability=0.2;
    protected double delta=0.05;
    protected double almostConvincedTreshold=0.7;

    public DiffusionVertexValue() {
        this.vertexThreshold=1;
        this.label=1;
    }
    
    public DiffusionVertexValue(int label) {
        this.vertexThreshold=1;
        this.label=label;
    }

    public DiffusionVertexValue( int vertexThreshold, int label) {
        this.vertexThreshold=vertexThreshold;
        this.label=label;
    }

    public void readFields(DataInput in) throws IOException {
        vertexThreshold = in.readInt();
        label = in.readInt();
        currentActivationProbability = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(vertexThreshold);
        out.writeInt(label);
        out.writeDouble(currentActivationProbability);
    }
    
    public double getCurrentActivationProbability() {
        return currentActivationProbability;
    }

    public void modifyCurrentActivationProbability(int sign) {
        BigDecimal tmpcurrentActivationProbability = new BigDecimal(currentActivationProbability).add(new BigDecimal(sign*delta)).setScale(5, RoundingMode.HALF_UP);
        if(tmpcurrentActivationProbability.doubleValue() > 1)
            currentActivationProbability = 1;
        else
            currentActivationProbability = tmpcurrentActivationProbability.doubleValue();
        if(tmpcurrentActivationProbability.doubleValue() <= 0)
            currentActivationProbability = 0;
    }

    public boolean isVertexInvited(int currentLabel) {
        return this.label >= currentLabel;

    }

    public boolean isVertexDead() {
        return new BigDecimal(currentActivationProbability).setScale(2, RoundingMode.HALF_DOWN).floatValue() == 0;
    }

    public boolean isVertexConvinced() {
        return new BigDecimal(currentActivationProbability).setScale(2, RoundingMode.HALF_DOWN).floatValue() == 1;
    }

    public void setVertexThreshold(int threshold) {
    	this.vertexThreshold=threshold;
    }
    
    public int getVertexThreshold() {
        return vertexThreshold;
    }

    public long getLabel() {
        return this.label;
    }

    public boolean rollActivationDice() {
        return Math.random() <= currentActivationProbability;
    }

    public void setlabel(int coreness) {
        this.label=coreness;
    }

    public boolean isAlmostConvinced(){
        return currentActivationProbability>almostConvincedTreshold;
    }

    // used at ss=0 in case of differences from default 0.2
    public void setInitialActivationProbability(double initialActivationProbability){
        this.currentActivationProbability=initialActivationProbability;
    }

    public void setAlmostConvincedTreshold(double almostConvincedTreshold){
        this.almostConvincedTreshold=almostConvincedTreshold;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }

    public String toString() {
        return ""+label+","+currentActivationProbability;
    }

}
