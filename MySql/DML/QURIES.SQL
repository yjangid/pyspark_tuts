-- SELECT * FROM EMPLOYEE
/*
EMP_ID	|NAME 	|ADDRESS| PHONE 	|SALARY |DEPT_ID
1		|Ramesh	|GNoida	|9855498465	|25000	|101
2		|Suresh	|GNoida	|9856549846	|75000	|102
3		|Rajesh	|GNoida	|9855497865	|28000	|103
4		|Shyamu	|BSB	|9853698465	|35000	|102
5		|Ramu	|BSB	|9855498235	|96000	|101
6		|Mahesh	|GNoida	|9851678465	|25000	|101
7		|Chaman	|BSBS	|9856723465	|215000	|103 */

-- WRITE QUERY TO GET MAX SALARY FROM EMP
SELECT MAX(SALARY), NAME FROM testdb.EMPLOYEE;
SELECT NAME, SALARY FROM testdb.EMPLOYEE ORDER BY SALARY DESC LIMIT 1

--------------------------------------------------------------------------------

-- GET SUM OF SALARY, BY DEPT ID
SELECT SUM(SALARY)AS SUM_OF_SALARY, DEPT_ID FROM testdb.employee GROUP BY DEPT_ID;

----------------------------------------------------------------------------

-- AVG SALARY
SELECT AVG(SALARY) AS AVG_SALARY, DEPT_ID FROM testdb.employee GROUP BY DEPT_ID;

----------------------------------------------------------------------------

-- CALCULATE MAX SALARY BY EACH DEPT
SELECT MAX(SALARY) AS SALARY, DEPT_ID FROM testdb.employee GROUP BY DEPT_ID;

----------------------------------------------------------------------------

-- GET HIGHEST SALRY FROM DEPT
SELECT MAX(SALARY) AS SALARY, DEPT_ID FROM testdb.employee GROUP BY DEPT_ID ORDER BY SALARY DESC LIMIT 1;

----------------------------------------------------------------------------

-- ROW_NUMBER () FUNCTION - The ROW_NUMBER() function in MySQL is used to returns the sequential number for each row within its partition
-- We can use the ROW_NUMBER() function to assign a sequence number for each record using the below statement:
-- ROW_NUMBER() OVER (<partition_definition> <order_definition>)  

SELECT *, ROW_NUMBER() OVER (PARTITION BY SALARY) AS ROW_NUM FROM testdb.EMPLOYEE;
--OUTPUT:- ROW_num is showing this beacuse we are creating partition over salary so it's assigning a values by unique pattern
/*
EMP_ID| NAME  |	ADDRESS| PHONE| 	 SALARY| DEPT_ID| ROW_NUM
1| 		Ramesh| GNoida|  9855498465| 25000|  101|	  1
6| 		Mahesh| GNoida|  9851678465| 25000|  101|	  2
3| 		Rajesh| GNoida|  9855497865| 28000|  103|	  1
4| 		Shyamu| BSB| 	 9853698465| 28000|  102|	  2
2| 		Suresh| GNoida|  98565498465|75000|  102|	  1
5| 		Ramu  |	BSB| 	 9855498235| 96000|  101|	  1
7| 		Chaman| BSBS|    9856723465| 215000| 103|	  1 
*/

----------------------------------------------------------------------------
SELECT *, ROW_NUMBER() OVER (ORDER BY SALARY DESC) AS ROW_NUM FROM testdb.EMPLOYEE;
--OUTPUT  --> HERE WE ARE NOT CREATING PARTIOTIONS, WE ARE JUST DOING ORDER BY HENCE EACH ROW ASSIGNED WITH UNIQUE ROW NUMBER
/* 
EMP_ID	|NAME	|ADDRESS	|PHONE		|SALARY	|DEPT_ID|ROW_NUM
1		|Ramesh	|GNoida		|9855498465	|25000	|101	|1
6		|Mahesh	|GNoida		|9851678465	|25000	|101	|2
3		|Rajesh	|GNoida		|9855497865	|28000	|103	|3
4		|Shyamu	|BSB		|9853698465	|28000	|102	|4
2		|Suresh	|GNoida		|98565498465|75000	|102	|5
5		|Ramu	|BSB		|9855498235	|96000	|101	|6
7		|Chaman	|BSBS		|9856723465	|215000	|103	|7 */

----------------------------------------------------------------------------
--------------- Get 3RD/NTH Highest salary --------------------------------
SELECT * FROM (SELECT *, ROW_NUMBER() OVER(ORDER BY SALARY DESC) AS ROW_NUM FROM testdb.employee) AS T
WHERE ROW_NUM=3;
--OUTPUT
/*
EMP_ID	NAME	ADDRESS	PHONE		SALARY	DEPT_ID	ROW_NUM
2		Suresh	GNoida	98565498465	75000	102		3

*/

------------ GET LAST 3 HIGHET SALARY ------------------------------------
SELECT * FROM (SELECT *, ROW_NUMBER() OVER(ORDER BY SALARY DESC) AS ROW_NUM FROM testdb.employee) as t
where ROW_NUM BETWEEN 2 AND 5;
/* OUTPUT
EMP_ID	|NAME	|ADDRESS	|PHONE		|SALARY	|DEPT_ID	|ROW_NUM
5		|Ramu	|BSB		|9855498235	|96000	|101		|2
2		|Suresh	|GNoida		|9856549846	|75000	|102		|3
3		|Rajesh	|GNoida		|9855497865	|28000	|103		|4
4		|Shyamu	|BSB		|9853698465	|28000	|102		|5 
*/
----------------------------------------------------------------------------
