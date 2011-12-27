package com.jackbe.mapreduce;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import com.jackbe.mapreduce.examples.stock.StockExampleEMML;

/**
 * This class is the entry point for running jobs and checking job status. It contains
 * the main method that is called by Hadoop when launched in a distributed cluster.
 *   
 * @author Christopher Steel - JackBe
 *
 * @since Jul 13, 2011 10:24:21 AM
 * @version 1.0
 */
public class EMMLMapReduce {
	private EMMLServiceType emmlServiceType = EMMLServiceType.LOCAL_EMML_SERVICE;
//	private EMMLServiceType emmlServiceType = EMMLServiceType.REST_EMML_SERVICE;
	private JobManagerType jobManagerType = JobManagerType.LOCAL_JOB_MANAGER;
	private String reducerScript;
	private String mapperScript;
	private String combinerScript;
	private JobManager jobManager;
	private EMMLRunner emmlRunner = null;
	private static Logger log = Logger.getLogger(EMMLMapReduce.class);
	private static volatile EMMLMapReduce instance = null;
	public static enum EMMLServiceType {REST_EMML_SERVICE, LOCAL_EMML_SERVICE};
	public static enum JobManagerType {LOCAL_JOB_MANAGER, REMOTE_JOB_MANAGER};


	protected EMMLMapReduce() {
		super();
		
//		this.setEmmlServiceType(EMMLServiceType.REST_EMML_SERVICE);
		this.setEmmlServiceType(EMMLServiceType.LOCAL_EMML_SERVICE);
		this.setJobManagerType(JobManagerType.LOCAL_JOB_MANAGER);

		this.setJobManagerType(JobManagerType.LOCAL_JOB_MANAGER);
		this.jobManager = new LocalJobManager();
	}
	
	/**
	 * Method for retrieving the Singleton instance.
	 *
	 * @return Singleton instance
	 */
	public static synchronized EMMLMapReduce getInstance() {
		//log.info("getInstance called.");
		if(instance == null) {
			try {
				instance = new EMMLMapReduce();
			} catch (Exception e) {
				log.error(e, e);
			}
		}
		return instance;
	}	
	
	public RunningJob start(String inputDir, String outputDir, String mapperScript, String reducerScript) throws Exception {
		
		if(log.isTraceEnabled())
			log.trace("called.");
		
		this.setMapperScript(mapperScript);
		this.setReducerScript(reducerScript);
		jobManager.setMapperScriptName(mapperScript);
		//jobManager.setCombinerScriptName(combinerScript);
		jobManager.setReducerScriptName(reducerScript);
		jobManager.init();
		return jobManager.startJob(inputDir, outputDir, mapperScript, reducerScript, null);
	}
	
	public RunningJob start(String inputDir, String outputDir, String mapperScript, String reducerScript, 
			String combinerScript) throws Exception {
		
		if(log.isTraceEnabled())
			log.trace("called.");
		
		this.setMapperScript(mapperScript);
		this.setReducerScript(reducerScript);	
		this.setCombinerScript(combinerScript);	
		
		return jobManager.startJob(inputDir, outputDir, mapperScript, reducerScript, combinerScript);
	}
	
	public String getAllJobs() {
		return jobManager.getAllJobs();
	}
	
	public String getJobState(RunningJob job) {
		try {
			return jobManager.getJobState(job);
		} catch (IOException e) {
			log.error("Exception getting job state: " + e, e);
		}	
		return "ERROR";
	}

	/**
	 * @return the emmlServiceType
	 */
	public EMMLServiceType getEmmlServiceType() {
		return emmlServiceType;
	}

	/**
	 * @param emmlServiceType the emmlServiceType to set
	 */
	public void setEmmlServiceType(EMMLServiceType emmlServiceType) {

		this.emmlServiceType = emmlServiceType;
		log.info("Setting service type to: " + emmlServiceType);

		if (emmlServiceType == EMMLServiceType.LOCAL_EMML_SERVICE) {
			try {
				log.info("Instantiating EMMLocalRunner");
				this.emmlRunner = new EMMLLocalRunner();
			} catch (Exception e) {
				log.error(
						"Could not create EMMLLocalRunner, defaulting to EMMLRestRunner: " + e, e);
				this.emmlRunner = null; //We want to blow up instead
			}
		}
		if (emmlServiceType == EMMLServiceType.REST_EMML_SERVICE) {
			log.info("Instantiating EMMRestRunner");
			this.emmlRunner = new EMMLRestRunner();
		}
	}

	/**
	 * @return the jobManagerType
	 */
	public JobManagerType getJobManagerType() {
		return jobManagerType;
	}

	/**
	 * @param jobManagerType the jobManagerType to set
	 */
	public void setJobManagerType(JobManagerType jobManagerType) {
		if(this.jobManagerType != jobManagerType) {
			this.jobManagerType = jobManagerType;
			if(jobManagerType == JobManagerType.LOCAL_JOB_MANAGER)
				this.jobManager = new LocalJobManager();
			if(jobManagerType == JobManagerType.REMOTE_JOB_MANAGER)
				this.jobManager = new LocalJobManager();
		}
	}

	public String getReducerScript() {
		// TODO Auto-generated method stub
		return this.reducerScript;
	}

	public void setReducerScript(String string) {
		System.out.println("Setting the reducer script to: " + string);
		log.error("Setting the reducer script to: " + string);
		this.reducerScript = string;		
	}	

	public String getMapperScript() {
		return this.mapperScript;
	}

	public void setMapperScript(String mapperScript) {
		System.out.println("Setting the mapper script to: " + mapperScript);
		log.error("Setting the mapper script to: " + mapperScript);
		this.mapperScript = mapperScript;
	}

	public String getCombinerScript() {
		return combinerScript;
	}

	public void setCombinerScript(String combinerScript) {
		this.combinerScript = combinerScript;
	}
	
	public EMMLRunner getEmmlRunner() {
		return this.emmlRunner;
	}
	
	/**
	 * The main method is called by Hadoop when the program is run in a Hadoop
	 * cluster and not just in stand-alone mode. Hadoop will copy the runnable jar
	 * file and invoke it, passing any needed comman-line arguments.
	 * 
	 * @param args Command line arguments needed by program. Usage:<p>
	 * java -jar com.jackbe..mapreduce.aws.EMMLMapReduce <inputDir> <outputDir> <mapperScript> <reducerScript> [<combinerScript>]
	 * <p>where <inputDir> is the directory containing the data files to be processed.<p>
	 * <outputDir> is the directory for Hadoop to put the result files.<p>
	 * <mapperScript> is the EMML text that maps a line of input (usually from a CSV file).<p>
	 * <reducerScript> is the EMML text that combines/reduces the mapped output to a file.
	 * [If no additional mapping is required, use the GenericMapper EMML script].<p> 
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		FileOutputStream fos = new FileOutputStream("/tmp/mapredlog." + System.currentTimeMillis());
		try {
			fos.write(("Starting EMMMpaReduce from commandline with args = " + args[0] + args[1] +args[2] + args[3] + "\n").getBytes());
			log.error("Starting EMML MapReduce from maincommandline with args = " + args);
		}
		catch(Exception e) {
		}
		finally {
			fos.close();
		}
		System.out.println("Starting EMML MapReduce from main.");
		log.error("Starting EMML MapReduce from main.");
		RunningJob job = null;
		EMMLMapReduce mapReduce = EMMLMapReduce.getInstance();
		mapReduce.setMapperScript("StockQuoteMapper");
		mapReduce.setReducerScript("StockAverageReducerIter");
		if (args == null || args.length < 2) {
			job = mapReduce.start("./input", "./output", null, null, null);
		} 
		else {
			if(args.length == 2) {
				job = mapReduce.start(args[0], args[1], null, null, null);
			}		
			if(args.length > 2) {
				job = mapReduce.start(args[0], args[1], args[3], args[4], null);
			}		
			if(args.length > 4) {
				job = mapReduce.start(args[0], args[1], args[3], args[4], args[5]);
			}

		}
		Thread.sleep(500);
		if(log.isInfoEnabled()) {
			log.info("Job ID: " + job.getID().getId());
			log.info("All Job IDs: " + mapReduce.getAllJobs());
		}
	} //main

}
