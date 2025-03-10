package ed.inf.adbs.blazedb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for BlazeDB.
 */
public class BlazeDBTest {


	@Before
	public void cleanOutputFile() throws Exception {
		Path outputFile = Paths.get("samples/output/output.csv");
		Files.deleteIfExists(outputFile);
	}

	// Test end-to-end execution of BlazeDB using the provided sample input and expected output files

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

	@Test
	public void query5Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query5.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query5.csv"));

		// We add this code to check results without considering the order of the tuples.
		Collections.sort(result);
		Collections.sort(expected);

		// Comparar las listas ordenadas
		assertEquals(expected, result);
	}

	@Test
	public void query6Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query6.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query6.csv"));

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void query7Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query7.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query7.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query8Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query8.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query8.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query9Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query9.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query9.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query10Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query10.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query10.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query11Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query11.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query11.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query12Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query12.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query12.csv"));

		assertEquals(expected, result);
	}

	// Add all cases that the CW requires

	// New case: Projections and selection at the same time
	@Test
	public void query13Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query13.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("50");

		assertEquals(expected, result);
	}

	@Test
	public void query14Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query14.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query14.csv"));

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void query15Test() throws Exception {
		System.out.println("Cross product - between 3 tables: ");
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query15.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query15.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query16Test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query16.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("200", "200", "100", "100", "100", "300");

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
