package ed.inf.adbs.blazedb;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CatalogTest {

    private Catalog catalog;

    @Before
    public void setUp() throws Exception {
        catalog = Catalog.getInstance();
        catalog.loadSchema("samples/db");
    }

   @Test
   public void testGetColumnIndex() {
       // Assuming the schema for students table is "id name age"

       assertEquals(0, catalog.getColumnIndex("Student", "A"));
       assertEquals(1, catalog.getColumnIndex("Student", "B"));
       assertEquals(2, catalog.getColumnIndex("Student", "C"));
   }


}
