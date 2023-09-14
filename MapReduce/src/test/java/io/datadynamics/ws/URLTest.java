package io.datadynamics.ws;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

public class URLTest {

	@Test
	public void uriTest() throws URISyntaxException, MalformedURLException {
//		URL url = new URL("http://www.data-dynamics.io");
//		URL _url = new URL("http://www.data_dynamics.io");
//		URL $url = new URL("http://www.data_dynamics.io");
		URI uri = URI.create("http://www.data-dynamics.io");
//		URI 0uri = URI.create("http://www.data-dynamics.io");
	}
	
	@Test
	public void minusModTest() {
		int result = -1 % 3;
		System.out.println(result);
	}
}
