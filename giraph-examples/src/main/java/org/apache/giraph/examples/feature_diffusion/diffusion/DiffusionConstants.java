package org.apache.giraph.examples.feature_diffusion.diffusion;

public class DiffusionConstants {

  // BROADCAST STRINGS
  public static final String superstepBroadcast = "CURR_SST";
  public static final String convincedVerticesAggregator = "CONV_AGG_DIFF";
  public static final String usingVerticesAggregator = "AGG_DIFF";
  public static final String deadVerticesAggregator = "AGG_DIFF_DEAD";
  public static final String latestActivationsAggregator = "AGG_ACTIVATED_LAST";
  public static final String potentialVerticesAggregator = "Potential_invited_vertices";
  public static final String oldInvitedVerticesAggregator = "Old_invited_vertices";
  public static final String oldConvincedVerticesAggregator = "Old_convinced_vertices";
  public static final String oldDeadVerticesAggregator = "Old_dead_vertices";
  public static final String oldUsingVerticesAggregator = "Old_using_vertices";
  public static final String oldHesitantVerticesAggregator = "Old_hesitant_vertices";
  public static final String oldAlmostConvincedVerticesAggregator = "Old_almostConvinced_vertices";
  public static final String justChangedTimeToSwitch = "Just_changed_timeToSwitch_value";
  public static final String timeToSwitchBroadcast = "is_time_to_switch";

  // GROUPS

  public static final String activatedVerticesCounterGroup = "Diffusion Counters";
  public static final String convincedVerticesCounter = "Convinced_Vertices ";
  public static final String usingVerticesCounter = "Using_Vertices ";
  public static final String deadVerticesCounter = "Dead_Vertices ";
  public static final String hesitantVerticesAggregator = "hesitantVerticesAggregator ";
  public static final String invitedVerticesAggregator = "Invited_Vertices ";
  public static final String almostConvincedVerticesAggregator = "AlmostConvinced_Vertices ";
  public static final String currentLabel =
      "Label_active "; // label da analizzare nello specifico superstep

  // OPTIONS
  public static final String diffusionDeltaOption = "diffusion.delta";
  public static final double diffusionDeltaOptionDefault = 0.005;
  public static final String diffusionListenOption = "diffusion.listenWhileUnactive";
  public static final String byLabelOption = "by_label";

  public double KSwitchTreshold;
}
