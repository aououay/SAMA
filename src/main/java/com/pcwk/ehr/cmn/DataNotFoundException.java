package com.pcwk.ehr.cmn;

public class DataNotFoundException extends RuntimeException {

	private static final long serialVersionUID = 274464773246042573L;

	public DataNotFoundException(String message) {
		super(message);
	}

}