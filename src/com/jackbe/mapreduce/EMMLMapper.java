package com.jackbe.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapred.JobConf;

/**
 * This class receives a line (or other piece of text) from the splitter adds it as an input
 * parameter and invokes the mapperEMML script. It outputs the result from the script using
 * the mapperKey.
 * 
 * @author Christopher Steel - JackBe
 *
 * @since Jul 14, 2011 1:01:42 AM
 * @version 1.0
 */
@SuppressWarnings("deprecation")
public class EMMLMapper extends MapReduceBase implements Mapper<LongWritable, Object, Text, Text> {
	static Logger log = Logger.getLogger(EMMLMapper.class);
	private EMMLMapReduce mapReduce;
	private EMMLRunner emmlRunner = null;
	
	public void map(LongWritable key, Object value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		try {
			mapReduce = EMMLMapReduce.getInstance();
			emmlRunner = mapReduce.getEmmlRunner();
			RuntimeException r = new RuntimeException("NO ERROR");
			log.error("Stack trace = " + r.getMessage(), r);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		if(log.isInfoEnabled())
			log.info("Value going into map script: " + value.toString());

		if(mapReduce.getMapperScript() == null) {
			//throw new IOException("ERROR: mapReduce.getMapperScript returned null");
			mapReduce.setMapperScript("HAMPModMapper");
		}
		
		emmlRunner.executeMapperScript(mapReduce.getMapperScript(), new Text(key.toString()),  value.toString(), output);

	} //map

	/**
	 * @return the emmlRunner
	 */
	public EMMLRunner getEmmlRunner() {
		return emmlRunner;
	}

	/**
	 * @param emmlRunner the emmlRunner to set
	 */
	public void setEmmlRunner(EMMLRunner emmlRunner) {
		this.emmlRunner = emmlRunner;
	}

} //class Map
