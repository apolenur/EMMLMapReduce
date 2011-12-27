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

import java.io.IOException;
import java.math.BigInteger;
import java.net.URLEncoder;
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

/**
 * @author Christopher Steel - JackBe
 *
 * @since Aug 7, 2011 6:47:51 PM
 * @version 1.0
 */
public class EMMLRestRunner implements EMMLRunner {
	private static Logger log = Logger.getLogger(EMMLRestRunner.class);
	private BigInteger counter = BigInteger.valueOf(0);
	private HttpClient client;
	private static final BigInteger one = BigInteger.valueOf(1);
//	private static String host = "ec2-50-17-132-69.compute-1.amazonaws.com";
	private static String host = "localhost";
	private static String port = "8080";
	private static String path = "/presto/edge/api/rest/";
	private static String operation = "runMashup";
	private static String format = "x-presto-resultFormat=xml";
	private static String protocol = "http://";
	private static String user = "admin";
	private static String password = "adminadmin";
	
	
	public EMMLRestRunner() {
		client = new HttpClient();
		client.getState().setCredentials(new AuthScope(host, Integer.parseInt(port)),
				new UsernamePasswordCredentials(user, password));

		HttpClientParams params = new HttpClientParams();
		params.setParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS, Boolean.TRUE);
		client.setParams(params);
		client.getParams().setAuthenticationPreemptive(true);
		throw new RuntimeException("TRYING TO CREATE EMMLRestRunner. BAD!");
	}
	
	@SuppressWarnings("deprecation")
	public void executeMapperScript(String scriptName, Text key, String value,
			OutputCollector<Text, Text> output) {

		if(log.isDebugEnabled())
			log.debug("Executing script: " + scriptName + " with key: " + key + " and values: \n----------------------------------\n" + value + "\n---------------------------------------------");

		if(scriptName == null) {
			scriptName = "HAMPModMapper";
			log.error("Hardcoded null scriptName tp MyStockQuoteMapper - FIX THIS.");
		}
		String resultData = null;		
		try {
			String encodedValue = URLEncoder.encode(value);
			resultData = executeRESTCall(scriptName, encodedValue);
			if(resultData == null) {
				log.error("Exception getting result data, executeMapperScript failed.");
				return;
			}
			
			String reducerKey = Utils.getMapReduceKey(resultData);
			if (reducerKey == null) {
				reducerKey = counter.add(one).toString();
			}
			else {
				if(log.isDebugEnabled()) 
					log.debug("Key from mapper result data: " + reducerKey);
			}
			
			// Strip XML header for each record.
			resultData = Utils.stripXmlHeader(resultData);
			if(log.isDebugEnabled()) {
				log.debug("Calling collect with result data: " + resultData);
			}
						
			output.collect(new Text(reducerKey), new Text(resultData));

		} 
		catch (Exception e) {
			log.error("Exception executing script: " + e, e);
		}		
	}	

	@Override
	public void executeReducerScript(String scriptName, Text key,
			Iterator<Text> iterator, OutputCollector<Text, Text> output) {

		if(log.isDebugEnabled())
			log.debug("Executing combiner/reducer script with key: " + key);

		if(scriptName == null) {
			scriptName = "HAMPModReducer";
			log.error("Hardcoded null scriptName tp MyStockQuoteReducer - FIX THIS.");
		}

		String resultData = null;		
		try {
			resultData = executeRESTCall(scriptName, null);

			String reducerKey = Utils.getMapReduceKey(resultData);
			if (reducerKey == null) {
				reducerKey = counter.add(one).toString();
			}
			else {
				if(log.isDebugEnabled()) 
					log.debug("Key from combiner/reducer result data: " + reducerKey);
			}
			
			if(log.isDebugEnabled()) {
				log.debug("Calling collect with result data: " + resultData);
			}
			
			// Strip XML header for each record.
			resultData = Utils.stripXmlHeader(resultData);
			
			output.collect(new Text(reducerKey), new Text(resultData));

		} catch (IOException e) {
			log.error("Exception executing combiner/reducer script: " + e, e);
		}
	}

	protected String executeRESTCall(String scriptName, String encodedValue) {

		// "http://localhost:8080/presto/edge/api/rest/StockQuoteMapper/runMashup?x-presto-resultFormat=xml&value=<encodedValue>"
		HttpMethod httpMethod = null;
		String result = null;

		if (encodedValue != null) {
			httpMethod = new GetMethod(protocol + host + ":" + port + path
					+ scriptName + "/" + operation + "?" + format + "&value="
					+ encodedValue);
			log.debug("Invoking REST service: " + protocol + host + ":" + port
					+ path + scriptName + "/" + operation + "?" + format + "&value="
					+ encodedValue);
		} 
		else {
			httpMethod = new GetMethod(protocol + host + ":" + port + path
					+ scriptName + "/" + operation + "?" + format);
			log.debug("Invoking REST service: " + protocol + host + ":" + port
					+ path + scriptName + "/" + operation + "?" + format);
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
			if (log.isDebugEnabled())
				log.debug("Response: " + result);
		} 
		catch (Exception e) {
			log.error("Exception executing REST call: " + e, e);
		} 
		finally {
			httpMethod.releaseConnection();
		}
		
		return result;
		
	}

	/**
	 * @param host the host to set
	 */
	public static void setHost(String host) {
		EMMLRestRunner.host = host;
	}

	/**
	 * @param port the port to set
	 */
	public static void setPort(String port) {
		EMMLRestRunner.port = port;
	}

	/**
	 * @param path the path to set
	 */
	public static void setPath(String path) {
		EMMLRestRunner.path = path;
	}

	/**
	 * @param operation the operation to set
	 */
	public static void setOperation(String operation) {
		EMMLRestRunner.operation = operation;
	}


	/**
	 * @param protocol the protocol to set
	 */
	public static void setProtocol(String protocol) {
		EMMLRestRunner.protocol = protocol;
	}
	
}
