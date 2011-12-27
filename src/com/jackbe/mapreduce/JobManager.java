package com.jackbe.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapred.RunningJob;

public interface JobManager {
	public RunningJob startJob(String inputDir, String outputDir, String mapperScript, String reducerScript, String combinerScript) throws Exception;
	/**
	 * This method returns the overall progress of a RunningJob. It retrieves the map progress
	 * and the reduce progress and returns the sum of those results / 2.
	 * 
	 * @param job
	 * @return the progress of the map and reduce tasks of a RunningJob as a number between 0.0 and 1.0
	 */
	public abstract float getJobProgress(RunningJob job);

	/**
	 * Utility method for retrieving Job progress using a String id instead of the 
	 * RunningJob.
	 * 
	 * @param id The RunningJon string identifier
	 * @return
	 */
	public abstract float getJobProgress(String id);

	public abstract String getJobState(RunningJob job) throws IOException;

	public abstract String getJobState(String id) throws IOException;

	public abstract String getAllJobs();

	/**
	 * Clears all jobs not in the <code>RUNNING</code> state for the status map.
	 * This should be called periodically to clean out old completed and failed jobs.
	 */
	public abstract void clearCompletedJobs();
	
	/**
	 * @return the reducerScriptName
	 */
	public String getReducerScriptName();

	/**
	 * @param reducerScriptName the reducerScriptName to set
	 */
	public void setReducerScriptName(String reducerScriptName);

	/**
	 * @return the combinerScriptName
	 */
	public String getCombinerScriptName();

	/**
	 * @param combinerScriptName the combinerScriptName to set
	 */
	public void setCombinerScriptName(String combinerScriptName);

	/**
	 * @return the mapperScriptName
	 */
	public String getMapperScriptName();

	/**
	 * @param mapperScriptName the mapperScriptName to set
	 */
	public void setMapperScriptName(String mapperScriptName);
	
	public void init();

}