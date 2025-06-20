package ed.inf.adbs.blazedb;

import static org.junit.Assert.assertEquals;

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
	public void query17() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query17.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList();

		assertEquals(expected, result);
	}

	@Test
	public void query18Test_SumMultiplicacion3Tables() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query18.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("30, 21, 3600, 9615100");

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

		Collections.sort(result);
		Collections.sort(expected);

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
	public void query23Test_JoinConditions3TablesNoOrder() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query23.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1",
				"1",
				"1",
				"2",
				"3",
				"4"
		);

		assertEquals(expected, result);
	}

	@Test
	public void query23Test_JoinConditions3TablesOrder() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query23A.sql", "samples/output/output.csv"});

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

	@Test
	public void query24() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query24.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 201, 90",
				"2, 201, 85"
		);

		assertEquals(expected, result);
	}

	@Test
	public void query25() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query25.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 500, 20, 15, 201, 90, 2, 3",
				"2, 500, 500, 30, 201, 85, 2, 3",
				"3, 300, 200, 40, 202, 88, 3, 4",
				"4, 300, 20, 10, 202, 92, 3, 4",
				"5, 400, 300, 50, 203, 70, 1, 1",
				"6, 600, 700, 20, 204, 95, 104, 2"
		);

		assertEquals(expected, result);
	}

	@Test
	public void query26() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query26.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 200",
				"2, 400",
				"3, 300",
				"4, 400",
				"5, 500",
				"6, 1800"
		);

		assertEquals(expected, result);
	}

	@Test
	public void query27_trivialQuery() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query27.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1",
				"2",
				"3",
				"4",
				"5",
				"6"
		);

		assertEquals(expected, result);
	}

	@Test
	public void query27A_trivialQuery() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query27A.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(

		);

		assertEquals(expected, result);
	}

	@Test
	public void query28() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query28.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6");

		assertEquals(expected, result);
	}

	@Test
	public void query29() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query29.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList("1");

		assertEquals(expected, result);
	}

	@Test
	public void query30JoinSameTable() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query30.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 101, 75, 1, 200, 50, 33, 101, 2, 3",
				"1, 102, 82, 1, 200, 50, 33, 102, 3, 4",
				"1, 103, 92, 1, 200, 50, 33, 103, 1, 1",
				"2, 101, 12, 2, 200, 200, 44, 101, 2, 3",
				"3, 102, 52, 3, 100, 105, 44, 102, 3, 4",
				"4, 104, 27, 4, 100, 50, 11, 104, 104, 2"
		);

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void query31_JoinNoOrder() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query31.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Files.readAllLines(Paths.get("samples/expected_output/query6.csv"));

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void query32_JoinNoOrder() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query32.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"2",
				"3",
				"4",
				"5",
				"6"
		);
		assertEquals(expected, result);
	}

	@Test
	public void query33_Complex() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query33.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 101, 1",
				"1, 102, 1",
				"1, 103, 1"

		);
		assertEquals(expected, result);
	}

	@Test
	public void query34_DistinctAllCol() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query34.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"1, 500, 20, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1",
				"2, 100, 100, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1"
		);
		assertEquals(expected, result);
	}

	@Test
	public void query35_SelectionSameTable() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query35.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"2, 200, 200, 44"
		);
		assertEquals(expected, result);
	}

	@Test
	public void query36_test() throws Exception {
		BlazeDB.main(new String[] {"samples/db", "samples/input2/query36.sql", "samples/output/output.csv"});

		List<String> result = Files.readAllLines(Paths.get("samples/output/output.csv"));
		List<String> expected = Arrays.asList(
				"2, 200, 200, 44"
		);
		assertEquals(expected, result);
	}


}
