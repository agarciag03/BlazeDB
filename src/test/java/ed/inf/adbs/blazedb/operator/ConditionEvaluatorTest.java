package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConditionEvaluatorTest {

    String tableName = "Student";
    String[] columnNames = {"A", "B", "C", "D"};

    @Before
    public void setUp() throws Exception {
        Catalog catalog = Catalog.getInstance();
        catalog.loadSchema("samples/db");
    }

    @Test
    public void testSimpleEqualsFalse() throws Exception {
        String conditionStr = "1 = 3";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertFalse(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testSimpleEqualsTrue() throws Exception {

        String conditionStr = "1 = 1";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertTrue(evaluator.evaluate(dummyTuple));
    }


    @Test
    public void testTrueEqualsReference() throws Exception {

        Tuple tuple = new Tuple("10, 200, 50, 33", tableName, columnNames);

        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.A = 10");
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(tuple));

        condition = CCJSqlParserUtil.parseCondExpression("Student.B = 200");
        evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(tuple));

        condition = CCJSqlParserUtil.parseCondExpression("Student.C = 50");
        evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(tuple));

        condition = CCJSqlParserUtil.parseCondExpression("Student.D = 33");
        evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(tuple));

    }

    @Test
    public void testFalseEqualsReference() throws Exception {
        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.B = 2");
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        assertFalse(evaluator.evaluate(new Tuple("1, 200, 50, 33", tableName, columnNames)));

    }

    @Test
    public void testNotEqualsTrue() throws Exception {
        String conditionStr = "1 != 2";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertTrue(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testNotEqualsFalse() throws Exception {
        String conditionStr = "1 != 1";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertFalse(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testNotEqualsWithReference() throws Exception {
        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.B != 10");
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));

        condition = CCJSqlParserUtil.parseCondExpression("Student.A != 10");
        evaluator = new ConditionEvaluator(condition);
        assertFalse(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));
    }

    @Test
    public void testGreaterThanTrue() throws Exception {
        String conditionStr = "3 > 1";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertTrue(evaluator.evaluate(dummyTuple));
    }


    @Test
    public void testGreaterThanFalseWithReference() throws Exception {
        String conditionStr = "1 > 3";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertFalse(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testGreaterThanWithReference() throws Exception {
        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.A > 9");
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));

        condition = CCJSqlParserUtil.parseCondExpression("Student.A > 10");
        evaluator = new ConditionEvaluator(condition);
        assertFalse(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));
    }

    @Test
    public void testGreaterThanOrEqualsTrue() throws Exception {
        String conditionStr = "3 >= 3";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertTrue(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testGreaterThanOrEqualsFalse() throws Exception {
        String conditionStr = "1 >= 3";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertFalse(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testGreaterThanOrEqualsWithReference() throws Exception {
        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.C >= 50");
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));

        condition = CCJSqlParserUtil.parseCondExpression("Student.D >= 32");
        evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));
    }

    @Test
    public void testLessThanTrue() throws Exception {
        String conditionStr = "1 < 3";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertTrue(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testLessThanFalse() throws Exception {
        String conditionStr = "3 < 1";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertFalse(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testLessThanOrEqualsTrue() throws Exception {
        String conditionStr = "3 <= 3";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertTrue(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testLessThanOrEqualsFalse() throws Exception {
        String conditionStr = "3 <= 1";
        Expression condition = CCJSqlParserUtil.parseCondExpression(conditionStr);
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        Tuple dummyTuple = new Tuple("1, 2, 3, 4", tableName, columnNames);
        assertFalse(evaluator.evaluate(dummyTuple));
    }

    @Test
    public void testGreaterThanOrEqualsReferenceWithSameReference() throws Exception {
        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.C >= Student.A");
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));

        condition = CCJSqlParserUtil.parseCondExpression("Student.A >= Student.C");
        evaluator = new ConditionEvaluator(condition);
        assertFalse(evaluator.evaluate(new Tuple("10, 200, 50, 33", tableName, columnNames)));

    }

    @Test
    public void testAndCondition2Options() throws Exception {
        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.A = 1 AND Student.B = 200");

        ConditionEvaluator evaluator = new ConditionEvaluator(condition);

        assertTrue(evaluator.evaluate(new Tuple("1, 200, 50, 33", tableName, columnNames)));
        assertFalse(evaluator.evaluate(new Tuple("1, 100, 50, 33", tableName, columnNames)));
        assertFalse(evaluator.evaluate(new Tuple("2, 200, 50, 33", tableName, columnNames)));
    }

    @Test
    public void testAndCondition4Options() throws Exception {

        Expression condition = CCJSqlParserUtil.parseCondExpression("Student.A = 1 AND Student.B = 200 AND Student.C = 50 AND Student.D >= 33");
        ConditionEvaluator evaluator = new ConditionEvaluator(condition);
        assertTrue(evaluator.evaluate(new Tuple("1, 200, 50, 33", tableName, columnNames)));
        assertFalse(evaluator.evaluate(new Tuple("1, 100, 50, 33", tableName, columnNames)));
        assertFalse(evaluator.evaluate(new Tuple("2, 200, 50, 33", tableName, columnNames)));
    }


}