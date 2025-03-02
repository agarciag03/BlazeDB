// SortOperatorTest.java
package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DummyOperator;
import ed.inf.adbs.blazedb.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SortOperatorTest {
    private DummyOperator dummyOperator;
    private List<Tuple> tuples;

    @Before
    public void setUp() throws Exception {
        this.tuples = Arrays.asList(
                new Tuple("4, 5, 6"),
                new Tuple("1, 2, 3"),
                new Tuple("7, 8, 9"),
                new Tuple("1, 2, 3"),
                new Tuple("4, 5, 6")

        );
        dummyOperator = new DummyOperator(tuples);


    }
    @Test
    public void testGetAllTuples() throws Exception {

        List<Tuple> tuples = Arrays.asList(
                new Tuple("1, 2, 3"),
                new Tuple("1, 2, 3"),
                new Tuple("4, 5, 6"),
                new Tuple("4, 5, 6"),
                new Tuple("7, 8, 9")
        );

        SortOperator sortOperator = new SortOperator(dummyOperator, Arrays.asList());
        dummyOperator.reset();
        List<Tuple> result = sortOperator.getAllTuples(dummyOperator);

        assertEquals(tuples, result);
    }

//    @Test
//    public void testSortTuples() throws Exception {
//
//       List<Integer> numbers = Arrays.asList(5, 3, 8, 1, 2);
//       Collections.sort(numbers);
//       System.out.println("Sorted numbers (natural ordering): " + numbers);
//
//       // Example with custom ordering
//        List<String> words = Arrays.asList("apple", "orange", "banana", "pear");
//        Collections.sort(words, new Comparator<String>() {
//            @Override
//            public int compare(String s1, String s2) {
//                return s2.compareTo(s1); // Sort in reverse order
//            }
//        });
//                System.out.println("Sorted words (custom ordering): " + words);
//        }

    @Test
    public void testSortTuples() throws Exception {
        List<Integer> orderByColumns = Arrays.asList(1);
        SortOperator sortOperator = new SortOperator(dummyOperator, orderByColumns);
        List<Tuple> sortedTuples = sortOperator.sortTuples(tuples);

        List<Tuple> expectedTuples = Arrays.asList(
                new Tuple("1, 2, 3"),
                new Tuple("1, 2, 3"),
                new Tuple("4, 5, 6"),
                new Tuple("4, 5, 6"),
                new Tuple("7, 8, 9")
        );

        assertEquals(expectedTuples, sortedTuples);
    }

}
