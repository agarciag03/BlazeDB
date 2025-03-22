# Task 1: Explanation about the logic used for extracting join conditions from WHERE clause.

For extracting join conditions from  the WHERE condition I considered the following steps:

1. In the class QueryPlanBuilder, there is a method called identifyElementsOperator. In this method, I identify the elements of the query mentioning steps. In the step 3, I identify the conditions in the WHERE clause. Since the WHERE Clause can have many expression concatenated by AND, I use a while to extract each WHERE expression and identify each of them.
2. In the same class, in the method identifyWhereExpression, I identify each condition of WHERE clause as a join, selection or trivial expression. Each condition is defined as:
* Joins: when there are two different table in the condition. For instance: Student.A = Enrolled.A
* Selections: when the condition involves just one table, in this case it could be one column or two columns in the same table. For instance: Student.A = 1 or Student.A = Student.C.
* Trivial expressions, when there values like 1= 1 or 2= 3 that end up being always true or false.

In particular, I identify the join conditions using the method isJoinCondition where I identify the expression on the left and right side of the WHERE condition. Then, if the left and right column of the WHERE condition are from different tables, it is classified and saved as a join condition.

3. Then, I identify join tables to extracting them from the FROM clause. It is in the same method identifyElementsOperator, as step 4. 
In this step, I identify the tables and I check if these tables have join conditions associated.
In order to validate if the tables have join conditions, I use the method extractJoinConditions where I check if any of the join conditions extracted in the last steps involves this table is. 
If there is not condition for this join table, this table is saved as a cross product.

4. Finally, when I have all the join conditions, I sort them in the method sortJoinConditions. Here, I verify whether the join conditions are in the correct order, because my program will apply the left tree, keeping the scanned tables on the left side and scanning the new tables on the right side. 
If the join does not follow this order, I swap the join condition. I also take into account that some comparisons like = or != are not affected when swapping the tables, but comparisons like >, <, >=, <=, are affected, so I need to modify the comparison operators as well.
   For example: From S, R Where R.A = S.A, here I swap the join transform it into S.A = R.A.


# Task 2: Optimisation rules/ Why they are correct / how they reduce the size of intermediate results during query evaluation.
Note: all the strategies mentioned here are in the code with a comment starting like: OPTIMISATION.

The steps that I considered for the optimization rules and reducing intermediate results are:

### Strategy 1: Trivial Expressions
1.	Before creating the query plan, I identify trivial expressions such as 1 = 1 or 2 = 3, which are always true or false. These expressions do not need to be considered in the query plan. If an expression is always true, I omit it from the plan. 
This strategy is correct as optimisation rules because If I find an always false expression, I can return an empty result immediately without even processing the query.

### Strategy 2: Selections and Projections
1. Selection Pushdown: When there are selections in the query, BlazeDB applies them immediately after scanning the table. This reduces the number of tuples passed to the next operator, ensuring that only the necessary tuples are processed by subsequent operators.
This strategy is applied in the method scanWithEarlyOptimisations and is called at the beginning of the query plan when the scan operator is created, as well as when a new scan operator is created after a join operator.

2. Projection Pushdown: When projections are included in the query, the program executes a method called identifyColumnsForEarlyProjections, which identifies the columns needed for all operators in the query and saves them in a list called NeededColumns.
After the selection pushdown is applied, the program marks the columns used in the selection as “noNeeded” for early projection. Then, the program applies the projection, keeping only the columns necessary for the next operators and the final result.

These steps are correct as optimization rules because selection pushdown reduces the number of tuples, and projection pushdown reduces the number of columns. Therefore, after applying these optimizations, the intermediate results are smaller, keeping only the necessary information for the subsequent operators.

### Strategy 3: Joins
1. Joins conditions are always executed after applying early projections and selections reduces the intermediate results by working with only the required tuples for this operator. 

2. After performing joins with conditions, the program applies all the possible projections and then execute the cross product. This step is aimed at minimizing the size of the cross product result. 


### Strategy 4 - Distinct before sorting
1. If the DISTINCT operator is needed in the query, the program applies it before the ORDER BY operator. This strategy reduces the number of tuples that need to be sorted by ORDER BY, keeping only the distinct tuples required for the final result.
Since ORDER BY is a blocking operator, having fewer tuples to process reduces memory usage, making sorting more efficient.  


# Note for the reviewer
* New queries were created for testing all the operators that the program can handle. These queries are in a new folder called input2.
* I also created new tables in the schema.txt and data files for testing purpose. 