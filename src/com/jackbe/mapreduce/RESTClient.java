/*
 * Copyright (©) 2011 JackBe Corporation
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of JackBe 
 * Corporation ("Confidential Information").  You shall not
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.log4j.Logger;

/**
 * @author Christopher Steel - JackBe Corporation
 *
 * @since Oct 12, 2011 2:38:05 PM
 * @version 1.0
 */
public class RESTClient {
	private static Logger log = Logger.getLogger(RESTClient.class);
	private HttpClient client;
	private String host = "ec2-50-17-132-69.compute-1.amazonaws.com";
//	private String host = "localhost";
	private String port = "8080";
	private String path = "/presto/edge/api";
//	private String operation = "runMashup";
	private String format = "x-presto-resultFormat=xml";
	private String protocol = "http://";
	private String user = "admin";
	private String password = "adminadmin";	

	public RESTClient() {
		client = new HttpClient();
		client.getState().setCredentials(new AuthScope(host, Integer.parseInt(port)),
				new UsernamePasswordCredentials(user, password));

		HttpClientParams params = new HttpClientParams();
		params.setParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS, Boolean.TRUE);
		client.setParams(params);
		client.getParams().setAuthenticationPreemptive(true);
		//throw new RuntimeException("TRYING TO CREATE EMMLRestRunner. BAD!");
	}
	
	public void executeREST(String encodedValue) {
		String result = null;
		String postPath = protocol + host + ":" + port + path;
		System.out.println("post path = " + postPath);
		PostMethod pm = new PostMethod(postPath);
		System.out.println("10 Encoded value = \n" + encodedValue);
		if (encodedValue != null) {
			//httpMethod = new PostMethod(protocol + host + ":" + port + path);
			//InputStream is = new ByteArrayInputStream(encodedValue.getBytes());
			//pm.setRequestBody(is);
			pm.setRequestBody(encodedValue);
			System.out.println("Validate = " + pm.validate());
			//pm.setQueryString(encodedValue);

			//pm.setHttp11(false);

			log.debug("Invoking REST service: " + protocol + host + ":" + port
						+ path + " " + pm.toString());

		} 
		else {
			throw new RuntimeException("EncodedValue can't be null");
		}
		
		try {
			client.executeMethod(pm);

			if (pm.getStatusCode() != HttpStatus.SC_OK) {
				log.error("HTTP Error status connecting to Presto: " + pm.getStatusCode());
				log.error("HTTP Error message connecting to Presto: " + pm.getStatusText());
				return;
			} 
			else {
				if (log.isDebugEnabled()) {
					log.debug("Status code: " + pm.getStatusCode());
					Header contentTypeHeader = pm.getResponseHeader("content-type");
					log.debug("Mimetype: " + contentTypeHeader.getValue());
				}
			}

			result = pm.getResponseBodyAsString();
			// log.debug(httpMethod.getStatusText());
			if (log.isDebugEnabled())
				log.debug("Response: " + result);
		} 
		catch (Exception e) {
			log.error("Exception executing REST call: " + e, e);
		} 
		finally {
			pm.releaseConnection();
		}
	}
	
	public void registerService(String url, String serviceName, String serviceID, String description) {
		//String body = "{\"version\": \"1.1\", \"svcVersion\": \"\", \"sid\": \"RDSService\", \"oid\": \"registerRESTService\", \"params\": [{\"name\": \"" + serviceName + "\", \"description\": \"" + description + "\", \"tags\": [], \"providerName\": \"NONE\", \"categories\": [], \"republish\": false, \"registerUsingAuthProtocol\": false, \"authProtocol\": \"\", \"authParams\": {\"map\": {}}}, \"" + url + "\", \"\", \"\"], \"header\": {\"invId\": \"\"}}";		
		String loginBody = "{\n\t\"version\": \"1.1\",\n" +
				"\t\"sid\": \"UserManagerService\",\n" +
				"\t\"svcVersion\": \"0.1\",\n" +
				"\t\"oid\": \"login\",\n" +
				"\t\"params\": [\n" +
					"\t\t\"admin\",\n" +
					"\t\t\"adminadmin\",\n" +
					"\t\t\"header\": {\"invId\": \"\"}\n" +
				"\t]\n" +
			"}";
		String body = "{\n\"version\": \"1.1\", " +
			"\"sid\": \"MetaRepositoryService\", " +
			"\"svcVersion\": \"0.1\", " +
			"\"oid\": \"ping\", " +
			"\"params\": [ " +
			"]\n" +
		"}";
		this.executeREST(loginBody);
	}
	
	public static void main(String[] args) {
		try {
			RESTClient client = new RESTClient();
			client.registerService(
					"http://ec2-50-17-132-69.compute-1.amazonaws.com:8080/presto/hadoop/out/part-00000",
					"ChrisRESTTest", "chrisid", "Test Description");
		} catch (Exception e) {
			System.out.println("Exception in main: " + e);
			e.printStackTrace();
		}
	}
}
