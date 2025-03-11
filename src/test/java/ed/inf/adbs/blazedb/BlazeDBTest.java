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
import java.util.stream.Collectors;

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
	public void query1Test_SelectAllColumns() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query1.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query1.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query2Test_Projection() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query2.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query2.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query3Test_Projection() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query3.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query3.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query4Test_Selection() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query4.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query4.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query5Test_Join() throws Exception {
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
	public void query6Test_Join() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query6.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query6.csv"));

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void query7Test_Distinct() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query7.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query7.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query8Test_OrderBy() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query8.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query8.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query9Test_SumGroup() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query9.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query9.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query10Test_SumConstGroup() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query10.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query10.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query11Test_GroupOrderBy() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query11.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query11.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query12Test_SumOnly() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input/query12.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query12.csv"));

		assertEquals(expected, result);
	}

	// Add all cases that the CW requires

	// New case: Projections and selection at the same time
	@Test
	public void query13Test_Selection() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query13.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("50");

		assertEquals(expected, result);
	}

	@Test
	public void query14JoinsSelections() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query14.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query14.csv"));

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void query15Test_CrossProduct() throws Exception {
		System.out.println("Cross product - between 3 tables: ");
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query15.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query15.csv"));

		assertEquals(expected, result);
	}

	@Test
	public void query16Test_GroupByProjection() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query16.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("200", "200", "100", "100", "100", "300");

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void query17Test_AllColAndSum() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query17.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1", "200", "50", "33", "1",  // A1
				"2", "200", "200", "44", "2", // A2
				"3", "100", "105", "44", "3", // A3
				"4", "100", "50", "11", "4",  // A4
				"5", "100", "500", "22", "5", // A5
				"6", "300", "400", "11", "6"  // A6
		);

		assertEquals(expected, result);
	}

	@Test
	public void query18Test_SumMultiplicacion3Tables() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query18.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("30, 21, 3600, 198407625");

		assertEquals(expected, result);
	}

	@Test
	public void query19Test_GroupByMultiplicacion() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query19.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"200, 600",
				"100, 1200",
				"300, 1800"
		);

		assertEquals(expected, result);
	}

	@Test
	public void query20Test_DistinctTable() throws Exception {
		BlazeDB.main(new String[]{"samples/db", "samples/input2/query20.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 200, 50, 33",
				"2, 200, 200, 44",
				"3, 100, 105, 44",
				"4, 100, 50, 11",
				"5, 100, 500, 22",
				"6, 300, 400, 11"
		);

		assertEquals(expected, result);
	}

	@Test
	public void query21Test_SelectEmptyTable() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query21.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Collections.emptyList(); // Expecting an empty list

		assertEquals(expected, result);
	}

	@Test
	public void query22Test_JoinEmptyTable() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query22.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Collections.emptyList(); // Expecting an empty result set due to empty Example table

		assertEquals(expected, result);
	}

	@Test
	public void query23Test_JoinConditions3Tables() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query23.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 200, 50, 33, 1, 101, 75, 101, 2, 3",
				"1, 200, 50, 33, 1, 102, 82, 102, 3, 4",
				"1, 200, 50, 33, 1, 103, 92, 103, 1, 1",
				"2, 200, 200, 44, 2, 101, 12, 101, 2, 3",
				"3, 100, 105, 44, 3, 102, 52, 102, 3, 4",
				"4, 100, 50, 11, 4, 104, 27, 104, 104, 2"
		);

		assertEquals(expected, result);
	}
}
