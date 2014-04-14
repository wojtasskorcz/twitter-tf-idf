package org.springframework.core;

/**
 * for spring data mongodb it can't determine the springversion in the shaded jar
 */
public class SpringVersion {
	public static String getVersion() {
		return "4.0.3-RELEASE";
	}

}