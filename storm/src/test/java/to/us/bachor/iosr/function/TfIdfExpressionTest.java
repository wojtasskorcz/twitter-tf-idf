package to.us.bachor.iosr.function;

import static org.junit.Assert.*;
import static to.us.bachor.iosr.TopologyNames.*;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TfIdfExpressionTest {

	private static final double EPS = 0.01;

	/* returns tf-idf value */
	private double getTfIdf(long tf, long d, long df) {
		TridentTuple tuple = Mockito.mock(TridentTuple.class);
		Mockito.when(tuple.getLongByField(D_TERM)).thenReturn(d);
		Mockito.when(tuple.getLongByField(DF_TERM)).thenReturn(df);
		Mockito.when(tuple.getLongByField(TF_TERM)).thenReturn(tf);
		TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
		@SuppressWarnings("unchecked")
		ArgumentCaptor<List<Object>> fooCaptor = ArgumentCaptor.forClass((Class<List<Object>>) (Class<?>) List.class);

		new TfidfExpression().execute(tuple, mockCollector);
		Mockito.verify(mockCollector, Mockito.times(1)).emit(fooCaptor.capture());
		return (double) fooCaptor.getValue().get(0);
	}

	private void checkTfIdf(long tf, long d, long df, double expectedTfIdf) {
		assertEquals(expectedTfIdf, getTfIdf(tf, d, df), 0.01);
	}

	@Test
	public void nanWhenNoDocuments() {
		checkTfIdf(0, 0, 0, Double.NaN);
	}

	@Test
	public void zeroTfImpliesZeroTfIdf() {
		checkTfIdf(0, 1, 1, 0.0);
		checkTfIdf(0, Long.MAX_VALUE, 1, 0.0);
	}

	@Test
	public void dEqualsDfImpliesZeroTfIdf() {
		checkTfIdf(100, 200, 200, 0.0);
		checkTfIdf(Long.MAX_VALUE, 200, 200, 0.0);
		checkTfIdf(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 0.0);
	}

	@Test
	public void changingTfChangesResultLinearly() {
		double forOne = getTfIdf(1, Long.MAX_VALUE, Long.MAX_VALUE / 2);
		double forHundred = getTfIdf(100, Long.MAX_VALUE, Long.MAX_VALUE / 2);
		Assert.assertTrue(Math.abs(forHundred - 100 * forOne) < EPS);
	}

	@Test
	public void biggerDfLowerTfIdf() {
		Assert.assertTrue(getTfIdf(1, Long.MAX_VALUE, 1) > getTfIdf(1, Long.MAX_VALUE, Long.MAX_VALUE));
	}
}
