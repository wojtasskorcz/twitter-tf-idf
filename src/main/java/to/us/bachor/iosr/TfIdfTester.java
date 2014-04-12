package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class TfIdfTester {

	public static void main(String[] args) throws TException, DRPCExecutionException {
		DRPCClient client = new DRPCClient("127.0.0.1", 3772);
		System.out.println("a");
		String result = client.execute(TF_IDF_QUERY, "http://t.co/hP5PM6fm have");
		System.out.println("b");
		System.out.println(result);
	}

}
