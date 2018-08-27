package org.apache.giraph.examples;


import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;


public class DiffusionMasterCompute extends DefaultMasterCompute {
	
	protected Logger LOG = Logger.getLogger(DiffusionMasterCompute.class); 
	
	public static final String convincedVerticesAggregator = "CONV_AGG_DIFF";
	public static final String usingVerticesAggregator = "AGG_DIFF";
	public static final String deadVerticesAggregator = "AGG_DIFF_DEAD";
	public static final String latestActivationsAggregator = "AGG_ACTIVATED_LAST";
	public static final String activatedVerticesCounterGroup = "Diffusion Counters";
	public static final String convincedVerticesCounter = "Convinced_Vertices ";	
	public static final String usingVerticesCounter = "Using_Vertices ";
	public static final String deadVerticesCounter = "Dead_Vertices ";
	public static final String diffusionDeltaOption = "diffusion.delta";
	public static final double diffusionDeltaOptionDefault = 0.005;
	public static final String diffusionListenOption = "diffusion.listenWhileUnactive";
	public static final String hesitantVerticesAggregator="hesitantVerticesAggregator ";
	
	
	public static final String byLabelOption="by_label";
	
	//for KCORE (or general label) based algorithm
	public static final String invitedVerticesAggregator="Invited_Vertices ";
	public static final String almostConvincedVerticesAggregator="AlmostConvinced_Vertices ";
	public static final String currentLabel="Label_active "; //label da analizzare nello specifico superstep
	public static final String nextLabel="Next_label ";  //ogni volta riceve tutte le label ancora da eseguire
	public static final String timeToSwitch="is_time_to_switch";
	public double KSwitchTreshold;

	//for MIN_NUMBER based algorithm
	public boolean byLabel;
	public int minNumber;
	public static final String potentialVerticesAggregator="Potential_invited_vertices";
	public static final String oldInvitedVerticesAggregator="Old_invited_vertices";
	public static final String oldConvincedVerticesAggregator="Old_convinced_vertices";
	public static final String oldDeadVerticesAggregator="Old_dead_vertices";
	public static final String oldUsingVerticesAggregator="Old_using_vertices";
	public static final String oldHesitantVerticesAggregator="Old_hesitant_vertices";
	public static final String oldAlmostConvincedVerticesAggregator="Old_almostConvinced_vertices";
	public static final String justChangedTimeToSwitch="Just_changed_timeToSwitch_value";


	@Override
	public void compute() {
		//super.compute();
		long convincedVertices = ((LongWritable)getAggregatedValue(convincedVerticesAggregator)).get();
		long usingVertices = ((LongWritable)getAggregatedValue(usingVerticesAggregator)).get();
		long deadVertices = ((LongWritable)getAggregatedValue(deadVerticesAggregator)).get();
		long invitedVertices=((LongWritable)getAggregatedValue(invitedVerticesAggregator)).get();
		long almostConvincedVertices=((LongWritable)getAggregatedValue(almostConvincedVerticesAggregator)).get();
		long activeLabel=(int)((LongWritable)getAggregatedValue(currentLabel)).get();
		long hesitantVerticesAggregatorVal=((LongWritable)getAggregatedValue(hesitantVerticesAggregator)).get();
		
		//This avoid having counters' value "0" when it's timeToSwitch and so the computation based on MIN_NUMBER is "paused"
		if(!byLabel && ((BooleanWritable)getAggregatedValue(timeToSwitch)).get()) {
			almostConvincedVertices=((LongWritable)getAggregatedValue(oldAlmostConvincedVerticesAggregator)).get();
			invitedVertices=((LongWritable)getAggregatedValue(oldInvitedVerticesAggregator)).get();
			usingVertices=((LongWritable)getAggregatedValue(oldUsingVerticesAggregator)).get();
			deadVertices=((LongWritable)getAggregatedValue(oldDeadVerticesAggregator)).get();
			convincedVertices=((LongWritable)getAggregatedValue(oldConvincedVerticesAggregator)).get();
			hesitantVerticesAggregatorVal=((LongWritable)getAggregatedValue(oldHesitantVerticesAggregator)).get();
		}
		
		getContext().getCounter(activatedVerticesCounterGroup,hesitantVerticesAggregator+superstep()).setValue(hesitantVerticesAggregatorVal);
		getContext().getCounter(activatedVerticesCounterGroup, usingVerticesCounter + superstep()).setValue(usingVertices);
		getContext().getCounter(activatedVerticesCounterGroup, convincedVerticesCounter + superstep()).setValue(convincedVertices);		
		getContext().getCounter(activatedVerticesCounterGroup, deadVerticesCounter + superstep()).setValue(deadVertices);
		getContext().getCounter(activatedVerticesCounterGroup,invitedVerticesAggregator + superstep()).setValue(invitedVertices);
		getContext().getCounter(activatedVerticesCounterGroup,almostConvincedVerticesAggregator + superstep()).setValue(almostConvincedVertices);
		getContext().getCounter(activatedVerticesCounterGroup,currentLabel + superstep()).setValue(activeLabel);

		
		
		//test purpose
		if(superstep()==0) {
			System.out.println("InitProb,"+getConf().getStrings("InitialProbability","0.02")[0]);
			System.out.println("Delta,"+getConf().getStrings("Delta","0.005")[0]);
		}else {
			System.out.println("InvitedVertices,"+(getSuperstep()-1)+","+invitedVertices);
			System.out.println("ConvincedVertices,"+(getSuperstep()-1)+","+convincedVertices);
			System.out.println("DeadVertices,"+(getSuperstep()-1)+","+deadVertices);
			System.out.println("AlmostConvincedVertices,"+(getSuperstep()-1)+","+almostConvincedVertices);
			System.out.println("UsingVertices,"+(getSuperstep()-1)+","+usingVertices);
			System.out.println("LabelReached,"+(getSuperstep()-1)+","+activeLabel);
			System.out.println("HesitantVertices,"+(getSuperstep()-1)+","+hesitantVerticesAggregatorVal);
		}

		
		if(getSuperstep() > 0 &&  getTotalNumVertices()==(deadVertices+convincedVertices+hesitantVerticesAggregatorVal ))
			haltComputation();
		
		if(byLabel) {//Kcore or similar
			if(getSuperstep()>0) {
				if (  ((BooleanWritable)getAggregatedValue(timeToSwitch)).get() )
					setAggregatedValue(timeToSwitch, new BooleanWritable(false));
				if ( superstep()==1) {
					setAggregatedValue(currentLabel, (LongWritable)getAggregatedValue(nextLabel));
					setAggregatedValue(nextLabel, new LongWritable(-1));
					setAggregatedValue(timeToSwitch, new BooleanWritable(true));
				}else if (activeLabel!=1){//if we haven't reached the lowest coreness
					long almostConvicedVal=((LongWritable) getAggregatedValue(DiffusionMasterCompute.almostConvincedVerticesAggregator)).get();
					long invitedVal=((LongWritable) getAggregatedValue(DiffusionMasterCompute.invitedVerticesAggregator)).get();				
					//if the threshold is reached or all the invited vertices are dead, convinced or hesitant
					if(((double)almostConvicedVal)/invitedVal>KSwitchTreshold || invitedVertices==(deadVertices+convincedVertices+hesitantVerticesAggregatorVal) ) {
						setAggregatedValue(timeToSwitch, new BooleanWritable(true));
						setAggregatedValue(currentLabel, (LongWritable)getAggregatedValue(nextLabel));
						setAggregatedValue(nextLabel, new LongWritable(-1));
					}
				}
			}
		}else {//degree, pagerank or other similar where the label does not represent a group of vertices
			if ( superstep()==0) {
				setAggregatedValue(currentLabel, new LongWritable(Long.MAX_VALUE));				
				setAggregatedValue(timeToSwitch, new BooleanWritable(true));
				setAggregatedValue(oldInvitedVerticesAggregator, new LongWritable(0));
			}
			if(superstep()>0) {
				
				if ( ! ((BooleanWritable)getAggregatedValue(timeToSwitch)).get() ) {
					if(activeLabel>0) {
						long almostConvicedVal=((LongWritable) getAggregatedValue(DiffusionMasterCompute.almostConvincedVerticesAggregator)).get();
						long invitedVal=((LongWritable) getAggregatedValue(DiffusionMasterCompute.invitedVerticesAggregator)).get();				
						//if the threshold is reached or all the invited vertices are dead, convinced or hesitant
						if(((double)almostConvicedVal)/invitedVal>KSwitchTreshold || invitedVertices==(deadVertices+convincedVertices+hesitantVerticesAggregatorVal)) {
							setAggregatedValue(timeToSwitch, new BooleanWritable(true));
							setAggregatedValue(oldInvitedVerticesAggregator, (LongWritable)getAggregatedValue(invitedVerticesAggregator));
							setAggregatedValue(oldConvincedVerticesAggregator, (LongWritable)getAggregatedValue(convincedVerticesAggregator));
							setAggregatedValue(oldAlmostConvincedVerticesAggregator, (LongWritable)getAggregatedValue(almostConvincedVerticesAggregator));
							setAggregatedValue(oldDeadVerticesAggregator, (LongWritable)getAggregatedValue(deadVerticesAggregator));
							setAggregatedValue(oldHesitantVerticesAggregator, (LongWritable)getAggregatedValue(hesitantVerticesAggregator));
							setAggregatedValue(oldUsingVerticesAggregator, (LongWritable)getAggregatedValue(usingVerticesAggregator));
						}
					}
				}else { //it's time to switch: let's scan some label until we find at least <minNumber> vertices more than now
					long old = ((LongWritable)getAggregatedValue(oldInvitedVerticesAggregator)).get();
					long actual = ((LongWritable)getAggregatedValue(potentialVerticesAggregator)).get();
					if (actual-old>minNumber) {//reached a label which increment the invited vertices by MIN_NUMBER at least
						setAggregatedValue(timeToSwitch, new BooleanWritable(false));
						setAggregatedValue(justChangedTimeToSwitch, new BooleanWritable(true));
					}else if( ((LongWritable)getAggregatedValue(nextLabel)).get()<0 && superstep()>10 ){//reached the lowest label without finding at least MIN_NUMBER vertices
						setAggregatedValue(timeToSwitch, new BooleanWritable(false));
						setAggregatedValue(justChangedTimeToSwitch, new BooleanWritable(true));
						setAggregatedValue(currentLabel, new LongWritable(0));
					}else {//continue to scan
						setAggregatedValue(currentLabel, (LongWritable)getAggregatedValue(nextLabel));
						setAggregatedValue(nextLabel, new LongWritable(-1));
					}
				}
			}
		}

		
	}

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		super.initialize();
		KSwitchTreshold = Double.parseDouble(getConf().getStrings("KSwitchThreshold", "0.7")[0]);
		registerAggregator(convincedVerticesAggregator, LongSumAggregator.class);		
		registerAggregator(usingVerticesAggregator, LongSumAggregator.class);		
		registerAggregator(deadVerticesAggregator, LongSumAggregator.class);	
		registerAggregator(invitedVerticesAggregator, LongSumAggregator.class);
		registerAggregator(almostConvincedVerticesAggregator, LongSumAggregator.class);
		registerPersistentAggregator(nextLabel, LongMaxAggregator.class);
		registerPersistentAggregator(currentLabel, LongMaxAggregator.class);
		registerPersistentAggregator(timeToSwitch, BooleanOrAggregator.class);
		registerAggregator(hesitantVerticesAggregator, LongSumAggregator.class);
		
		byLabel=Boolean.parseBoolean(getConf().getStrings("ByLabel", "true")[0]);
		registerPersistentAggregator(byLabelOption, BooleanOrAggregator.class);
		setAggregatedValue(byLabelOption, new BooleanWritable(byLabel));
		if(!byLabel) {
			minNumber = Integer.parseInt(getConf().getStrings("minNumber", "200")[0]);
			registerAggregator(potentialVerticesAggregator, LongSumAggregator.class);
			registerPersistentAggregator(oldInvitedVerticesAggregator, LongSumAggregator.class);
			registerPersistentAggregator(oldConvincedVerticesAggregator, LongSumAggregator.class);
			registerPersistentAggregator(oldDeadVerticesAggregator, LongSumAggregator.class);
			registerPersistentAggregator(oldUsingVerticesAggregator, LongSumAggregator.class);
			registerPersistentAggregator(oldHesitantVerticesAggregator, LongSumAggregator.class);
			registerPersistentAggregator(oldAlmostConvincedVerticesAggregator, LongSumAggregator.class);
			registerAggregator(justChangedTimeToSwitch, BooleanOrAggregator.class);

		}
	}
	
	protected long superstep() {
		return getSuperstep();
	}

}
