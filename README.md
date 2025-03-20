# Task 1: Explanation about the logic used for extracting join conditions from WHERE clause.

For extracting join conditions from  the WHERE condition I considered the following steps:


strategy for extracting join conditions from the WHERE clause and evaluating
them as part of the join.

in a method 
1. Extract the conditions from the WHERE clause.
here we identify 3 types of conditions:
* Joins: when there are two different table in the condition. For instance: Student.A = Enrolled.A
* Selections: Here we can find conditions that involves just one table, in this case it could be one columns or two columns in the same table. For instance: Student.A = 1 or Student.A = Student.C. We 
2. Identify the join order through the joins in the parsing.
3. Organise the joins in order to just to left to right joins based on the order of the tables in the FROM clause.
4. Check if the join is in a correct way in terms of order of the join, to apply the left tree, keeping the scanned tables on the left side and scanning the new tables on the right side, so swapping them if it is needed. For example: From S, R Where R.A = S.A, here I swap the join transform it into S.A = R.A. 
5. Identify the join conditions  because comparisons like = or != are not affected when swapping the tables, but comparisons like >, <, >=, <=, are affected, so I need to swap the comparison operators as well.


# Task 2: Optimisation rules/ Why they are correct / how they reduce the size of intermediate results during query evaluation.

The steps that I considered for the optimization rules and reducing intermediate results are:
transform query plans:
swap operators in the following order

0. Check trivial select 
1. Selections pushdown: Where there are selections, BlazeDB will apply selections just after scanning the table. This will reduce the number of tuples that are passed to the next operator, guaranteeing that just tuples needed will be processed by the next  operator. 
2. new instances - Projection Pushdown: Where there are projections in the query....
* Projections before joins can reduce intermediate results working just in the columns needed.
* Be carefull projections should be apply at the beginning but also at the end, because there could be columns that are needed for operators like joins, groupby, orderby, etc and they should be taken away from the intermediate results at the end of the query.
 - This is to leave the result as user is expecting and reduce the intermediate results before distinct and order by operators.
4. distinctOperator before order by to reduce the number of tuples that are passed to this operator that in fact block the whole database
5. new instances or projections to give the result that user is expecting
6. sort is a blocking operator, which
   means it really needs to see all of its input before producing any output  - Order by is the last operator to apply and it is only apply where it is required in order to avoid unnecessary sorting of the tuples. We can leave the sorting at the end since the instructions "You may also assume that the attributes mentioned in the ORDER BY are a subset of
   those retained by the SELECT. This allows you to do the sorting last, after projection." Otherwise we should check if a new projection would be needed to release the results that the user is expecting. It is good to delay sorting as late as possible, in
   particular to do it after the projection(s), because there will be less data to sort that way.

or introduce new instances of the non-join operators discussed above