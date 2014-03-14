package to.us.bachor.iosr;

import org.junit.Test;

public class BasicStormTest {
	@Test
	public void runRandomNumbersCountingTopology() {
		new RandomCountingTopology().runTopologyLocally();
	}
}
