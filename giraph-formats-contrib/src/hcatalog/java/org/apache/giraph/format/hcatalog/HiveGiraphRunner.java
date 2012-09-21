/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.format.hcatalog;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HiveGiraphRunner implements Tool {

	@SuppressWarnings("rawtypes")
	private Class<? extends Vertex> vertexClass;
	@SuppressWarnings("rawtypes")
	private Class<? extends HCatalogVertexInputFormat> vertexInputFormatClass;
	@SuppressWarnings("rawtypes")
	private Class<? extends HCatalogVertexOutputFormat> vertexOutputFormatClass;

	protected HiveGiraphRunner(
			@SuppressWarnings("rawtypes") Class<? extends Vertex> vertexClass,
			@SuppressWarnings("rawtypes") Class<? extends HCatalogVertexInputFormat> vertexInputFormatClass,
			@SuppressWarnings("rawtypes") Class<? extends HCatalogVertexOutputFormat> vertexOutputFormatClass) {
		this.vertexClass = vertexClass;
		this.vertexInputFormatClass = vertexInputFormatClass;
		this.vertexOutputFormatClass = vertexOutputFormatClass;
		this.conf = new HiveConf(getClass());
	}

	protected String dbName;
	protected String inputTableName;
	protected String inputTableFilterExpr;
	protected String outputTableName;
	protected Map<String, String> outputTablePartitionValues;

	protected int workers;
	protected boolean isVerbose;

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner
				.run(new HiveGiraphRunner(null, null, null), args));
	}

	@Override
	public final int run(String[] args) throws Exception {
		// process args
		try {
			processArguments(args);
		} catch (InterruptedException e) {
			return 0;
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			return -1;
		}

		// additional configuration for Hive
		adjustConfigurationForHive(getConf());

		// setup GiraphJob
		GiraphJob job = new GiraphJob(getConf(), getClass().getName());
		job.setVertexClass(vertexClass);

		// setup input from Hive
		InputJobInfo inputJobInfo = InputJobInfo.create(dbName, inputTableName,
				inputTableFilterExpr);
		HCatInputFormat.setInput(job.getInternalJob(), inputJobInfo);
		job.setVertexInputFormatClass(vertexInputFormatClass);

		// setup output to Hive
		HCatOutputFormat.setOutput(job.getInternalJob(), OutputJobInfo.create(
				dbName, outputTableName, outputTablePartitionValues));
		HCatOutputFormat.setSchema(job.getInternalJob(),
				HCatOutputFormat.getTableSchema(job.getInternalJob()));
		job.setVertexOutputFormatClass(vertexOutputFormatClass);

		job.setWorkerConfiguration(workers, workers, 100.0f);
		initGiraphJob(job);

		return job.run(isVerbose) ? 0 : -1;
	}

	private static void adjustConfigurationForHive(Configuration conf) {
		// when output partitions are used, workers register them to the
		// metastore at cleanup stage, and on HiveConf's initialization, it
		// looks for hive-site.xml from.
		addToStringCollection(conf, "tmpfiles", conf.getClassLoader()
				.getResource("hive-site.xml").toString());

		// Also, you need hive.aux.jars as well
		// addToStringCollection(conf, "tmpjars",
		// conf.getStringCollection("hive.aux.jars.path"));

		// Or, more effectively, we can provide all the jars client needed to
		// the workers as well
		String[] hadoopJars = System.getenv("HADOOP_CLASSPATH").split(
				File.pathSeparator);
		List<String> hadoopJarURLs = Lists.newArrayList();
		for (String jarPath : hadoopJars) {
			File file = new File(jarPath);
			if (file.exists() && file.isFile()) {
				String jarURL = file.toURI().toString();
				hadoopJarURLs.add(jarURL);
			}
		}
		addToStringCollection(conf, "tmpjars", hadoopJarURLs);
	}

	private CommandLine processArguments(String[] args) throws ParseException,
			InterruptedException {
		Options options = new Options();
		options.addOption("h", "help", false, "Help");
		options.addOption("v", "verbose", false, "Verbose");
		options.addOption("D", "hiveconf", true,
				"property=value for Hive/Hadoop configuration");
		options.addOption("w", "workers", true, "Number of workers");
		if (vertexClass == null)
			options.addOption(null, "vertexClass", true,
					"Giraph Vertex class to use");
		if (vertexInputFormatClass == null)
			options.addOption(null, "vertexInputFormatClass", true,
					"Giraph HCatalogVertexInputFormat class to use");
		if (vertexOutputFormatClass == null)
			options.addOption(null, "vertexOutputFormatClass", true,
					"Giraph HCatalogVertexOutputFormat class to use");
		options.addOption("db", "database", true, "Hive database name");
		options.addOption("i", "inputTable", true, "Input table name");
		options.addOption("I", "inputFilter", true,
				"Input table filter expression (e.g., \"a<2 AND b='two'\"");
		options.addOption("o", "outputTable", true, "Output table name");
		options.addOption("O", "outputPartition", true,
				"Output table partition values (e.g., \"a=1,b=two\")");
		addMoreOptions(options);

		CommandLineParser parser = new GnuParser();
		final CommandLine cmdln = parser.parse(options, args);
		// for (Option opt : cmd.getOptions()) {
		// System.out.println(" opt -" + opt.getOpt() + " " + opt.getValue());
		// }
		if (args.length == 0 || cmdln.hasOption("help")) {
			new HelpFormatter().printHelp(getClass().getName(), options, true);
			throw new InterruptedException();
		}

		// Giraph classes
		if (cmdln.hasOption("vertexClass"))
			vertexClass = findClass(cmdln.getOptionValue("vertexClass"),
					Vertex.class);
		if (cmdln.hasOption("vertexInputFormatClass"))
			vertexInputFormatClass = findClass(
					cmdln.getOptionValue("vertexInputFormatClass"),
					HCatalogVertexInputFormat.class);
		if (cmdln.hasOption("vertexOutputFormatClass"))
			vertexOutputFormatClass = findClass(
					cmdln.getOptionValue("vertexOutputFormatClass"),
					HCatalogVertexOutputFormat.class);

		if (vertexClass == null)
			throw new IllegalArgumentException(
					"Need the Giraph Vertex class name (-vertexClass) to use");
		if (vertexInputFormatClass == null)
			throw new IllegalArgumentException(
					"Need the Giraph VertexInputFormat class name (-vertexInputFormatClass) to use");
		if (vertexOutputFormatClass == null)
			throw new IllegalArgumentException(
					"Need the Giraph VertexOutputFormat class name (-vertexOutputFormatClass) to use");

		if (!cmdln.hasOption("workers"))
			throw new IllegalArgumentException(
					"Need to choose the number of workers (-w)");
		if (!cmdln.hasOption("inputTable"))
			throw new IllegalArgumentException(
					"Need to set the input table name (-i).  One example is 'dim_friendlist'");
		if (!cmdln.hasOption("outputTable"))
			throw new IllegalArgumentException(
					"Need to set the output table name (-o).");

		dbName = cmdln.getOptionValue("dbName", "default");
		inputTableName = cmdln.getOptionValue("inputTable");
		inputTableFilterExpr = cmdln.getOptionValue("inputFilter");
		outputTableName = cmdln.getOptionValue("outputTable");
		outputTablePartitionValues = parsePartitionValues(cmdln
				.getOptionValue("outputPartition"));
		workers = Integer.parseInt(cmdln.getOptionValue("workers"));
		isVerbose = cmdln.hasOption("verbose");

		// pick up -hiveconf arguments
		Configuration conf = getConf();
		for (String hiveconf : cmdln.getOptionValues("hiveconf")) {
			String[] keyval = hiveconf.split("=", 2);
			if (keyval.length == 2) {
				String name = keyval[0];
				String value = keyval[1];
				if (name.equals("tmpjars") || name.equals("tmpfiles"))
					addToStringCollection(conf, name, value);
				else
					conf.set(name, value);
			}
		}

		processMoreArguments(cmdln);

		return cmdln;
	}

	private static void addToStringCollection(Configuration conf, String name,
			String... values) {
		addToStringCollection(conf, name, Arrays.asList(values));
	}

	private static void addToStringCollection(Configuration conf, String name,
			Collection<? extends String> values) {
		Collection<String> tmpfiles = conf.getStringCollection(name);
		tmpfiles.addAll(values);
		conf.setStrings(name, tmpfiles.toArray(new String[tmpfiles.size()]));
		// System.out.println(name + "=" + conf.get(name));
	}

	private <T> Class<? extends T> findClass(String className, Class<T> base) {
		try {
			Class<?> cls = Class.forName(className);
			if (base.isAssignableFrom(cls))
				return cls.asSubclass(base);
			return null;
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(className
					+ ": Invalid class name");
		}
	}

	// TODO use Hive util class if this is already provided by it
	public static Map<String, String> parsePartitionValues(
			String outputTablePartitionString) {
		if (outputTablePartitionString != null) {
			Map<String, String> partitionValues = Maps.newHashMap();
			for (String partkeyval : outputTablePartitionString.split(" *, *")) {
				String[] keyval = partkeyval.split(" *= *", 2);
				if (keyval.length < 2)
					throw new IllegalArgumentException(
							"Unrecognized partition value format: "
									+ outputTablePartitionString);
				partitionValues.put(keyval[0], keyval[1]);
			}
			return partitionValues;
		} else
			return null;
	}

	private static String serializePartitionValues(
			Map<String, String> outputTablePartitionValues) {
		StringBuilder outputTablePartitionValuesString = new StringBuilder();
		for (Entry<String, String> partitionValueEntry : outputTablePartitionValues
				.entrySet()) {
			if (outputTablePartitionValuesString.length() != 0)
				outputTablePartitionValuesString.append(",");
			outputTablePartitionValuesString
					.append(partitionValueEntry.getKey()).append("=")
					.append(partitionValueEntry.getValue());
		}
		return outputTablePartitionValuesString.toString();
	}

	/** Configuration */
	private Configuration conf;

	@Override
	public final Configuration getConf() {
		return conf;
	}

	@Override
	public final void setConf(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * Override this method to add more command-line options. You can process
	 * them by also overriding {@link #processMoreArguments(CommandLine)}.
	 * 
	 * @param options
	 */
	protected void addMoreOptions(Options options) {
	}

	/**
	 * Override this method to process additional command-line arguments. You
	 * may want to declare additional options by also overriding
	 * {@link #addMoreOptions(Options)}.
	 * 
	 * @param cmd
	 */
	protected void processMoreArguments(CommandLine cmd) {
	}

	/**
	 * Override this method to do additional setup with the GiraphJob that will
	 * run.
	 * 
	 * @param job
	 *            GiraphJob that is going to run
	 */
	protected void initGiraphJob(GiraphJob job) {
		System.out.println(getClass().getSimpleName() + " with");
		String prefix = "\t";
		System.out.println(prefix + "-vertexClass="
				+ vertexClass.getCanonicalName());
		System.out.println(prefix + "-vertexInputFormatClass="
				+ vertexInputFormatClass.getCanonicalName());
		System.out.println(prefix + "-vertexOutputFormatClass="
				+ vertexOutputFormatClass.getCanonicalName());
		System.out.println(prefix + "-inputTable=" + inputTableName);
		if (inputTableFilterExpr != null)
			System.out.println(prefix + "-inputFilter=\""
					+ inputTableFilterExpr + "\"");
		System.out.println(prefix + "-outputTable=" + outputTableName);
		if (outputTablePartitionValues != null)
			System.out.println(prefix + "-outputPartition=\""
					+ serializePartitionValues(outputTablePartitionValues)
					+ "\"");
		System.out.println(prefix + "-workers=" + workers);
	}

}
