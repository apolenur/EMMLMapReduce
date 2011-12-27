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

import java.math.BigInteger;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;

/**
 * This class provides static helper methods for processing MapReduce jobs.
 * @author Christopher Steel - JackBe
 *
 * @since Aug 7, 2011 7:02:15 PM
 * @version 1.0
 */
public class Utils {
	private static Logger log = Logger.getLogger(Utils.class);
	protected static String MAP_REDUCE_KEY_BEGIN_TAG = "<MapReduceKey>";
	protected static String MAP_REDUCE_KEY_END_TAG = "</MapReduceKey>";
	public static final String XML_HEADER_SEQUENCE = "<?xml version=";

	public static String getMapReduceKey(String xml) {
		if(log.isTraceEnabled())
			log.trace("Getting " + MAP_REDUCE_KEY_BEGIN_TAG + " from: " + xml);
		
		String key = null;
		if (xml != null && xml.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) > -1) {
			try {
				key = xml.substring(
						xml.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) + MAP_REDUCE_KEY_BEGIN_TAG.length(),
						xml.indexOf(MAP_REDUCE_KEY_END_TAG));
			} 
			catch (Exception e) {
				if(log.isInfoEnabled())
					log.info("MapReduce key not properly formatted, giving up.");
			}
		}
		return key; 		
	}
	
	public static String stripXmlHeader(String xml) {
		if(log.isTraceEnabled())
			log.trace("called.");
		
		String strippedXml = null;
		if(xml != null && xml.startsWith(XML_HEADER_SEQUENCE)) {
			strippedXml = xml.substring(xml.indexOf(">") + 1);
			if(log.isDebugEnabled())
				log.debug("Stripping XML header.");
			return strippedXml;
		}
		return xml;
	}
	
}
