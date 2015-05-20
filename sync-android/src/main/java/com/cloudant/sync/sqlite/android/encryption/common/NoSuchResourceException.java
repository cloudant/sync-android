/*
 * IBM Confidential OCO Source Materials
 * 
 * 5725-I43 Copyright IBM Corp. 2006, 2013
 * 
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has
 * been deposited with the U.S. Copyright Office.
 * 
*/

package com.cloudant.sync.sqlite.android.encryption.common;

public class NoSuchResourceException extends RuntimeException {
	
	public NoSuchResourceException(String string, Exception e) {
		super(string, e);
	}

	private static final long serialVersionUID = 7561423853842800353L;

}
