package ed.inf.adbs.blazedb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for BlazeDB.
 */
public class BlazeDBTest {

	/**z
	 * Rigorous Test :-)
	 */
	@Test
	public void shouldAnswerWithTrue() {
		assertTrue(true);
	}

	@Test
	public void query1Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query1.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query1.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query2Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query2.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query2.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query3Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query3.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query3.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query4Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query4.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query4.csv"));

		assertEquals(expected, result);
	}

	// Add all cases that the CW requires

	// New case: Projections and selection at the same time
	@Test
	public void query13Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query13.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("50");

		assertEquals(expected, result);
	}

	// Different cases
//	@Test
//	public void queryExample() throws Exception {
//		BlazeDB.main(new String[] {"samples/db", "samples/input/example.sql", "samples/output/output.csv"});
//
//		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
//		List<String> expected = Arrays.asList("50");
//
//		assertEquals(expected, result);
//	}


}
