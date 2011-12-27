/*
 * Copyright (©) 2011 FortMoon Consulting
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of FortMoon
 * Consulting Corporation ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with FortMoon.
 *
 * FORTMOON MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE
 * SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, OR NON-INFRINGEMENT. FORTMOON SHALL NOT BE LIABLE FOR ANY DAMAGES
 * SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.jackbe.mapreduce;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.log4j.Logger;

/**
 * @author Christopher Steel - JackBe
 *
 * @since Aug 7, 2011 11:57:55 AM
 * @version 1.0
 */
@SuppressWarnings("deprecation")
public class LocalJobManager implements JobManager {
	private static Logger log = Logger.getLogger(LocalJobManager.class);
	protected HashMap<String, RunningJob> statusMap = new HashMap<String, RunningJob>();
	protected JobConf conf = null;
	protected JobClient jobClient;
	private String mapperScriptName;
	private String reducerScriptName;
	private String combinerScriptName;
	protected static String NAMENODE = "ip-10-244-163-80";
	public static final String JOB_NOT_FOUND = "Job Not Found";

	//Comment
	public LocalJobManager() {
	}
	
	public void init() {

	    conf = new JobConf(LocalJobManager.class);

		//conf.set("mapred.map.child.java.opts", "-Djava.library.path=/");
		//conf.set("mapred.reduce.child.java.opts", "-Djava.library.path=/");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.set("mapred.job.tracker","ip-10-244-163-80:45365");
//		conf.set("mapred.job.tracker","ec2-50-16-77-94.compute-1.amazonaws.com:45365");
		conf.setMapperClass(EMMLMapper.class);
		conf.setReducerClass(EMMLReducer.class);
		conf.setJar("/home/ec2-user/hadoopmapreduce.jar");
		conf.set("mapper.scriptname", this.mapperScriptName);
		conf.set("reducer.scriptname", this.reducerScriptName);
		//conf.set("combiner.scriptname", this.combinerScriptName);
		
		//conf.setNumTasksToExecutePerJvm(100);
		//conf.setNumMapTasks(250);
		//conf.setNumTasksToExecutePerJvm(50);
		//conf.setNumReduceTasks(50);
		conf.setInputFormat(TextInputFormat.class);
		//Use our own XML formatter
		conf.setOutputFormat(XmlOutputFormat.class);  	

	    try {
			jobClient = new JobClient(JobTracker.getAddress(conf), conf);
		} catch (IOException e) {
			log.error("Exception creating jobClient: " + e, e);
		}
	}
	
	public RunningJob startJob(String inputDir, String outputDir, String mapperScript, String reducerScript, String combinerScript) throws Exception {

		init();
		conf.setJobName("EMMLMapReduce");
		//conf.setSessionId(Long.toString(System.currentTimeMillis()));
		
		conf.set("MAPPER_SCRIPT", mapperScript);
		conf.set("REDUCER_SCRIPT", reducerScript);
		if(combinerScript != null) {
			conf.set("COMBINER_SCRIPT", combinerScript);
			conf.setCombinerClass(EMMLCombiner.class);
		}
		
		
//		FileInputFormat.setInputPaths(conf, new Path(inputDir));
		FileInputFormat.setInputPaths(conf, new Path("hdfs://" + NAMENODE + "/" + inputDir));
//		FileOutputFormat.setOutputPath(conf, new Path(outputDir));
		Path outputPath = new Path("hdfs://" + NAMENODE + "/" + outputDir);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(conf, outputPath);
		RESTRegistrationJobCallback callback = new RESTRegistrationJobCallback(outputPath, outputDir, conf);		
		
		RunningJob job = null;
		try {
			job = jobClient.submitJob(conf);
			this.registerJobCompleteCallback(job, callback);

			statusMap.put(job.getJobID(), job);
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		jobClient.getSystemDir();
		return job;
	}
	
	public void registerJobCompleteCallback(RunningJob job, JobCompleted callback) {
		final RunningJob rJob = job;
		final JobCompleted jc = callback;
		Runnable r = new Runnable() {
			@Override
			public void run() {
				boolean complete = false;
				while(!complete) {
					try {
						complete = rJob.isComplete();
					} catch (IOException ex) {
						log.error("Exception calling isComplete: " + ex, ex);
						return;
					}
					try {
						// Sleep 5 seconds and then check isComplete() again.
						Thread.sleep(5000l);
					} 
					catch (InterruptedException e) {
					}
				}
				jc.complete(rJob);	
			}
		};
		Thread t = new Thread(r);
		t.start();
	}
	
	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.JobManager#getJobProgress(org.apache.hadoop.mapred.RunningJob)
	 */
	@Override
	public float getJobProgress(RunningJob job) {
		if(log.isTraceEnabled())
			log.trace("called.");
		
		if(job == null) {
			return -1.0f;
		}
		
		try {
			if (job.mapProgress() == 0.0f) 
				return 0.0f;
			// Return the progress as the combination of the map and reduce task progress.
			//TODO: Figure out a better way to convert reduce progress time to mapper time.
			return ((job.mapProgress() + job.reduceProgress())/2.0f)*100.0f;
		} 
		catch (IOException e) {
			log.error("Exception getting job progress: " + e, e);
			return 0.0f;
		}

	}
	
	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.JobManager#getJobProgress(java.lang.String)
	 */
	@Override
	public float getJobProgress(String id) {
		RunningJob job = null;
		try {
			job = jobClient.getJob(JobID.forName(id));
		} catch (Exception e) {
			log.error("Exception get JobID for job: " + id + " :" + e ,e);
			return 0.0f;
		}
		return getJobProgress(job);
	}
	
	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.JobManager#getJobState(org.apache.hadoop.mapred.RunningJob)
	 */
	@Override
	public String getJobState(RunningJob job) throws IOException {
		if(log.isTraceEnabled())
			log.trace("called.");
		
		if(job == null)
			return JOB_NOT_FOUND;

		return JobStatus.getJobRunState(job.getJobState());
	}

	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.JobManager#getJobState(java.lang.String)
	 */
	@Override
	public String getJobState(String id) throws IOException {
		if(log.isTraceEnabled())
			log.trace("called.");

		RunningJob job = null;
		try {
			job = jobClient.getJob(JobID.forName(id));
		} catch (Exception e) {
			log.error("Exception get JobID for job: " + id + " :" + e ,e);
			return JOB_NOT_FOUND;
		}
		return getJobState(job);
	}
	
	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.JobManager#getAllJobs()
	 */
	@Override
	public String getAllJobs() {
		RunningJob[] jobsInfo = null;

		try {
			if(jobClient == null)
				return "<Exception> NULL jobClient. </Exception>";
			//FIXME need to fix for remote jobs
			//jobsInfo = jobClient.getAllJobs();
		} catch (Exception e) {
			log.error("Exception getting jobs from jobClient: " + e , e);
			return "Exception getting jobs: " + e.getMessage();
		}
		// If this is running local, we need to use the Jobs in our map.
		// TODO: remove old entries eventually.
		
		RunningJob[] temp = new RunningJob[statusMap.size()];
		jobsInfo = statusMap.values().toArray(temp);
		
		StringBuffer xml = new StringBuffer();
		xml.append("<jobs>\n");
		for(RunningJob job : jobsInfo) {
			try {
				xml.append("<job>\n\t<id>" + job.getID().toString() + "</id>\n\t<state>" + JobStatus.getJobRunState(job.getJobState()));
			} catch (IOException e) {
				log.error("Exception apending job status info: " + e);
				// The XML string is now screwed up, just return null.
				return null;
			}
			xml.append("</state>\n\t<progress>" + getJobProgress(job.getID().toString()) + "</progress>\n</job>\n");
		}
		xml.append("\n</jobs>");
		return xml.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.JobManager#clearCompletedJobs()
	 */
	@Override
	public void clearCompletedJobs() {
		if(log.isTraceEnabled())
			log.trace("called.");
		
		for(String key : statusMap.keySet()) {
			RunningJob job = statusMap.get(key);
			try {
				if(job.getJobState() != JobStatus.RUNNING)
					statusMap.remove(key);
			} catch (IOException e) {
				log.error("Exception clearing completed jobs: " + e, e);
			}
		}
	}
	
	private void copyToLocal(String hdfsPath, String localPath) {
		Path hPath = new Path("hdfs://" + NAMENODE + "/" + hdfsPath);
		Path lPath = new Path(localPath);
		try {
			hPath.getFileSystem(conf).copyToLocalFile(hPath, lPath);
		} catch (IOException e) {
			log.error("Could not copy from " + hPath.toString() + " to " + lPath.toString() + ": " + e.getMessage(), e);
		}
		
	}

	/**
	 * @return the reducerScriptName
	 */
	public String getReducerScriptName() {
		return reducerScriptName;
	}

	/**
	 * @param reducerScriptName the reducerScriptName to set
	 */
	public void setReducerScriptName(String reducerScriptName) {
		this.reducerScriptName = reducerScriptName;
	}

	/**
	 * @return the combinerScriptName
	 */
	public String getCombinerScriptName() {
		return combinerScriptName;
	}

	/**
	 * @param combinerScriptName the combinerScriptName to set
	 */
	public void setCombinerScriptName(String combinerScriptName) {
		this.combinerScriptName = combinerScriptName;
	}

	/**
	 * @return the mapperScriptName
	 */
	public String getMapperScriptName() {
		return mapperScriptName;
	}

	/**
	 * @param mapperScriptName the mapperScriptName to set
	 */
	public void setMapperScriptName(String mapperScriptName) {
		this.mapperScriptName = mapperScriptName;
	}
}
