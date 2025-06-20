# BlazeBD: lightweight database management system

BlazeDB is a SQL interpreter built entirely in Java that emulates fundamental parts of a relational database system. It executes SQL queries on a file-based database by parsing, analyzing, and delivering results based on relational algebra principles — all without depending on any external database software.


## Key Features
	•	SQL Query Parsing using JSQLParser
	•	Query Execution Pipeline based on the iterator model, enabling tuple-at-a-time processing
	•	Query Optimizations:
 

## Optimisation rules:
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

