/*
 * Copyright (©) 2011 JackBe Corporation
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of FortMoon
 * Consulting Corporation ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with JackBe.
 *
 * JACKBE MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE
 * SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, OR NON-INFRINGEMENT. FORTMOON SHALL NOT BE LIABLE FOR ANY DAMAGES
 * SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.jackbe.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;


/**
 * @author Christopher Steel - JackBe Corporation
 *
 * @since Oct 10, 2011 12:17:18 PM
 * @version 1.0
 */
public class RESTRegistrationJobCallback implements JobCompleted {
	private Configuration conf;
	private Path hdfsOutDir = null;
	private String localOutDir = null;
	private Logger log = Logger.getLogger(RESTRegistrationJobCallback.class);
	private static String LOCAL_PATH_PREFIX = "../webapps/presto/hadoop/";
	private static String LOCAL_FILE_NAME = "part-00000";
	private static String HDFS_FILE_NAME = "part-00000";
	
	@SuppressWarnings("unused")
	private RESTRegistrationJobCallback() {
		
	}
	
	public RESTRegistrationJobCallback(Path hdfsOutput, String localOutput, Configuration conf) {
		this.hdfsOutDir = hdfsOutput;
		this.localOutDir = LOCAL_PATH_PREFIX + localOutput;
		this.conf = conf;
	}
	
	@Override
	public void complete(RunningJob job) {

		File outputDirPath = new File(this.localOutDir);
		if(outputDirPath.exists()) {
			deleteAllFiles(outputDirPath);
			boolean deleted = outputDirPath.delete();
			if(deleted && log.isDebugEnabled())
				log.debug("Deleted output directory."); 
		}
		
		try {
			Path hPath = new Path(this.hdfsOutDir.toUri() + "/" + HDFS_FILE_NAME);
			if(log.isDebugEnabled())
				log.debug("Copying " + hPath + " to " + this.localOutDir + "/" + LOCAL_FILE_NAME);
			
			this.hdfsOutDir.getFileSystem(this.conf).copyToLocalFile(
					hPath, new Path(this.localOutDir + "/" + LOCAL_FILE_NAME));
		} catch (IOException e) {
			log.error("Exception copying output from hdfs path: " + this.hdfsOutDir.toString() + 
					" to local path: " + this.localOutDir + " :" + e, e);
			e.printStackTrace();
		}
	}
	
	private void deleteAllFiles(File path) {
		if(!path.isDirectory())
			return;
		
		for (String filename : path.list()) {
			File file = new File(path.getAbsolutePath() + "/" + filename);
			if (file.isDirectory()) {
				deleteAllFiles(file);
			} 
			else {
				if (log.isDebugEnabled())
					log.debug("Trying to delete file: " + file.getPath());

				boolean deletedFile = file.delete();
				if (log.isDebugEnabled()) {
					if (deletedFile)
						log.debug("Deleted file: " + file.getPath());
					else
						log.debug("Could not delete file: " + file.getPath());
				}
			}
		}
	}

}
