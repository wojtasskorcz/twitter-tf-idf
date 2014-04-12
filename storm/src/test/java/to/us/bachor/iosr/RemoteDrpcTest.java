package to.us.bachor.iosr;

import static org.junit.Assert.*;
import static to.us.bachor.iosr.TopologyNames.*;

import org.apache.thrift7.TException;
import org.junit.Test;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class RemoteDrpcTest {

	@Test
	public void mockedDrpc() throws TException, DRPCExecutionException {
		DRPCClient client = new DRPCClient("127.0.0.1", 3772);
		String term = "have";
		String result = client.execute(TF_IDF_QUERY, MOCK_URL + " " + term);
		String expected = String.format("[[\"%s\",\"%s\",16.0]]", MOCK_URL.replace("/", "\\/"), term);
		assertEquals(expected, result);
	}

}
