# Task 1: Explanation about the logic used for extracting join conditions from WHERE clause.

For extracting join conditions from  the WHERE condition I considered the following steps:

1. Extract the conditions from the WHERE clause.
2. Identify the join order through the joins in the parsing.
3. Organise the joins in order to just to left to right joins based on the order of the tables in the FROM clause.
4. Check if the join is in a correct way in terms of order of the join, to apply the left tree, keeping the scanned tables on the left side and scanning the new tables on the right side, so swapping them if it is needed. For example: From S, R Where R.A = S.A, here I swap the join transform it into S.A = R.A. 
5. Identify the join conditions  because comparisons like = or != are not affected when swapping the tables, but comparisons like >, <, >=, <=, are affected, so I need to swap the comparison operators as well.


# Task 2: Optimisation rules/ Why they are correct / how they reduce the size of intermediate results during query evaluation.

The steps that I considered for the optimization rules and reducing intermediate results are:

1. Selections pushdown: Where there are selections, BlazeDB will apply selections just after scanning the table. This will reduce the number of tuples that are passed to the next operator, guaranteeing that just tuples needed will be processed by the next  operator. 
2. Projection Pushdown: Where there are projections in the query....
* Projections before joins can reduce intermediate results working just in the columns needed.
* Be carefull projects should be apply at the beginning but also at the end, because there could be columns that are needed for operators like joins, groupby, orderby, etc and they should be taken away from the intermediate results at the end of the query.

