package com.jackbe.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;
import org.oma.emml.me.runtime.EMMLService;
import org.oma.emml.me.runtime.EMMLServiceFactory;
import org.oma.emml.me.runtime.ExecutionContext;
import org.oma.emml.me.runtime.MashupResponse;
import org.oma.emml.me.runtime.expr.EmmlVariable;

/**
 * @author Christopher Steel - JackBe
 *
 * @since Aug 29, 2011 11:39:10 AM
 * @version 1.0
 */
public class EMMLLocalRunner implements EMMLRunner {
	private static Logger log = Logger.getLogger(EMMLLocalRunner.class);
	private long counter = System.currentTimeMillis();
	private HttpClient client;
	protected EMMLService emmlService = null;
	private final static String MAP_REDUCE_KEY_BEGIN_TAG = "<MapReduceKey>";
	private final static String MAP_REDUCE_KEY_END_TAG = "</MapReduceKey>";
	private final static String host = "ec2-50-17-132-69.compute-1.amazonaws.com";
	private final static String port = "8080";
	private static final String user = "admin";
	private static final String password = "adminadmin";
	
	public EMMLLocalRunner() throws Exception {
		try {
			emmlService = EMMLServiceFactory.createService();		
		} catch (Exception e) {
			log.error("Exception creating EMMLService: " + e, e);
			throw e;
		}
		client = new HttpClient();
		client.getState().setCredentials(new AuthScope(host, Integer.parseInt(port)),
				new UsernamePasswordCredentials(user, password));

		HttpClientParams params = new HttpClientParams();
		params.setParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS, Boolean.TRUE);
		client.setParams(params);
		client.getParams().setAuthenticationPreemptive(true);

	}
	
	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.EMMLRunner#executeMapperScript(java.lang.String, org.apache.hadoop.io.Text, java.lang.String, org.apache.hadoop.mapred.OutputCollector)
	 */
	@Override
	public void executeMapperScript(String scriptName, Text key, String value,
			OutputCollector<Text, Text> output) {
		
		String resultData = null;
		
		if(log.isDebugEnabled())
			log.debug("Executing mapper script with key: " + key + " and value: " + value);

		HashMap<String, String> inputParams = new HashMap<String, String>();
		inputParams.put("key", key.toString());
		inputParams.put("value", value);
		
		if(scriptName == null) {
			scriptName = "HAMPModMapper";
			log.error("Hardcoded null scriptName tp MyStockQuoteReducer - FIX THIS.");
			//throw new RuntimeException("Mapper script name is null. Aborting executeMapperScript.");
		}
		
		String emml = this.getEMML(scriptName);
		log.debug("Mapper script EMML:\n" + emml);
		MashupResponse mashupResponse;
		try {
			mashupResponse = emmlService.executeMashupScript(new ExecutionContext(), emml, inputParams);
			EmmlVariable result = mashupResponse.getResult();
			resultData = result.getDataAsString();
		}
		catch (Exception e) {
			log.error("Exception executing mapper script: " + e, e);
			return;
		}
		
		// Try and retrieve the MapReduceKey from the result data
		String mapReduceKey = null;
		
		if (resultData != null && resultData.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) > -1) {
			try {
				mapReduceKey = resultData.substring(
						resultData.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) + 14,
						resultData.indexOf(MAP_REDUCE_KEY_END_TAG));
			} catch (Exception e) {
				if(log.isInfoEnabled())
					log.info("Exception getting key: " + e);
				mapReduceKey = Long.toString(counter++);
			}
		} 
		else {
			mapReduceKey = Long.toString(counter++);
		}
		
		if(log.isDebugEnabled()) {
			log.debug("Key from result data = " + mapReduceKey + " for script: " + scriptName);
			log.debug("Calling collect with result date = " + resultData);
		}

		try {
			output.collect(new Text(mapReduceKey), new Text(resultData));
		} catch (IOException e) {
			log.error("Exception executing mapper output.collect: " + e, e);
		}

	}

	/* (non-Javadoc)
	 * @see com.jackbe.mapreduce.EMMLRunner#executeReducerScript(java.lang.String, org.apache.hadoop.io.Text, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector)
	 */
	@Override
	public void executeReducerScript(String scriptName, Text key,
			Iterator<Text> iterator, OutputCollector<Text, Text> output) {
		
		String resultData = null;
		
		if(log.isDebugEnabled())
			log.debug("Executing reducer script with key: " + key);
		
		if(scriptName == null) {
			scriptName = "HAMPModReducer";
			log.error("Hardcoded null scriptName tp MyStockQuoteReducer - FIX THIS.");
		}
		
		HashMap<String, String> inputParams = new HashMap<String, String>();
		inputParams.put("key", key.toString());

		String emml = this.getEMML(scriptName);

		try {
			MashupResponse mashupResponse = emmlService.executeMashupScript(new ExecutionContext(), emml, inputParams);
			EmmlVariable result = mashupResponse.getResult();
			resultData = result.getDataAsString();
		}
		catch (Exception e) {
			log.error("Exception executing reducer script: " + e, e);
		}
		
		// Try and retrieve the MapReduceKey from the result data
		String mapReduceKey = null;
		
		if (resultData != null && resultData.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) > -1) {
			try {
				mapReduceKey = resultData.substring(
						resultData.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) + 14,
						resultData.indexOf(MAP_REDUCE_KEY_END_TAG));
			} catch (Exception e) {
				if(log.isInfoEnabled())
					log.info("Exception getting key: " + e);
				mapReduceKey = Long.toString(counter++);
			}
		} 
		else {
			mapReduceKey = Long.toString(counter++);
		}
		
		if(log.isDebugEnabled()) {
			log.debug("Key from result data = " + mapReduceKey + " for script: " + scriptName);
			//log.debug("Calling collect with result date = " + resultData);
		}
		log.warn("Calling collect with result date = " + resultData);
		System.out.println("Calling collect with result date = " + resultData);
		// Strip the XML header
		resultData = Utils.stripXmlHeader(resultData);
		
		try {
			output.collect(new Text(mapReduceKey), new Text(resultData));
		} catch (IOException e) {
			log.error("Exception executing reducer output.collect: " + e, e);
		}
	}
	
	protected String getEMML(String scriptName) {

		// "http://ec2-50-17-132-69.compute-1.amazonaws.com:8080/presto/edge/api/rest/JEMSDesigner/getClientScript?MyStockQuoteMapper?MyStockQuoteMapper>"
		HttpMethod httpMethod = null;
		String result = null;

		if (scriptName != null) {
			httpMethod = new GetMethod("http://ec2-50-17-132-69.compute-1.amazonaws.com:8080/presto/edge/api/rest/JEMSDesigner/getClientScript?"+ scriptName);
			log.debug("Invoking REST service: http://ec2-50-17-132-69.compute-1.amazonaws.com:8080/presto/edge/api/rest/JEMSDesigner/getClientScript?" + scriptName);
		} 
		else {
			log.debug("Error Invoking REST service: script name is null");
			return null;
		}
		
		try {
			client.executeMethod(httpMethod);

			if (httpMethod.getStatusCode() != HttpStatus.SC_OK) {
				log.error("HTTP Error status connecting to Presto: " + httpMethod.getStatusCode());
				log.error("HTTP Error message connecting to Presto: " + httpMethod.getStatusText());
				return null;
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Status code: " + httpMethod.getStatusCode());
					Header contentTypeHeader = httpMethod.getResponseHeader("content-type");
					log.debug("Mimetype: " + contentTypeHeader.getValue());
				}
			}

			result = httpMethod.getResponseBodyAsString();
			// log.debug(httpMethod.getStatusText());
			//if (log.isDebugEnabled())
			//	log.debug("Response: " + result);
		} 
		catch (IOException e) {
			log.error("Exception executing REST call: " + e, e);
		} 
		finally {
			httpMethod.releaseConnection();
		}
		
		return result;
		
	}


}
