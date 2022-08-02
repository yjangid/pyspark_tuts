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

-- ROW_NUMBER(), RANK(), DENSE_RANK() DIFFERENCE
-- QUERY - 
SELECT *,ROW_NUMBER() OVER(ORDER BY SALARY) AS ROW_NUM,
RANK() OVER(ORDER BY SALARY) AS RANK_D,
DENSE_RANK() OVER(ORDER BY SALARY) AS DENSE
FROM testdb.EMPLOYEE;

/*
EMP_ID	|NAME	|ADDRESS	PHONE		|SALARY	|DEPT_ID	|ROW_NUM	|RANK_D	|DENSE
1		|Ramesh	|GNoida		|9855498465	|25000	|101		|1			|1		|1
6		|Mahesh	|GNoida		|9851678465	|25000	|101		|2			|1		|1
3		|Rajesh	|GNoida		|9855497865	|28000	|103		|3			|3		|2
4		|Shyamu	|BSB		|9853698465	|28000	|102		|4			|3		|2
2		|Suresh	|GNoida		|9856549846	|75000	|102		|5			|5		|3
5		|Ramu	|BSB		|9855498235	|96000	|101		|6			|6		|4
7		|Chaman	|BSBS		|9856723465	|215000	|103		|7			|7		|5
*/

-------------------------FIND DUPLICATES----------------------------------------
SELECT COUNT(*) AS COUNT, SALARY FROM testdb.EMPLOYEE GROUP BY SALARY
/* HERE COUNT IS SHOWING HOWMANY DUPLICATES VALUES ARE PRESENT
COUNT	|SALARY
2		|25000
1		|75000
2		|28000
1		|96000
1		|215000
*/

---------------------FIND ONLY DUPLICATES VALUES BY COLUMN ---------------------
SELECT COUNT(*) AS COUNT, SALARY FROM testdb.EMPLOYEE GROUP BY SALARY HAVING COUNT>1;
/*
COUNT	|SALARY
2		|25000
2		|28000
*/

------------------FIND RECORDS WHICH ARE DUPLICATE-------------------------------
----- FIRST UNDERSTAND THE SCENARIOS --------------------------------------------
-------IN THIS CASE WE WILL USE SELF JOIN BY INNER JOIN--------------------------
--LET'S UNDERSTAND THIS QUERY FIRST.. WE ARE JOING BOTH THE TABLES BY SALARY COLUMNS,
-- THE RESULT LOOKS LIKE----
SELECT A.NAME, A.SALARY, B.NAME, B.SALARY FROM testdb.EMPLOYEE A INNER JOIN testdb.EMPLOYEE B
ON A.SALARY = B.SALARY;
/* JOINED BASED ON SALARY
A_NAME	|A_SAL	|B_NAME	|B_SAL
Mahesh	|25000	|Ramesh	|25000
Ramesh	|25000	|Ramesh	|25000
Suresh	|75000	|Suresh	|75000
Shyamu	|28000	|Rajesh	|28000
Rajesh	|28000	|Rajesh	|28000
Shyamu	|28000	|Shyamu	|28000
Rajesh	|28000	|Shyamu	|28000
Ramu	|96000	|Ramu	|96000
Mahesh	|25000	|Mahesh	|25000
Ramesh	|25000	|Mahesh	|25000
Chaman	|215000 |Chaman	|215000
*/

-- LET'S ADD ONE CONDITION WHICH WILL GIVE US DESIRE RESULT ---
SELECT A.NAME AS A_NAME, A.SALARY AS A_SAL, B.NAME AS B_NAME, B.SALARY AS B_SAL FROM testdb.EMPLOYEE A INNER JOIN testdb.EMPLOYEE B
ON A.SALARY = B.SALARY
WHERE A.NAME<> B.NAME;
/*
A_NAME	|A_SAL	|B_NAME	|B_SAL
Mahesh	|25000	|Ramesh	|25000
Shyamu	|28000	|Rajesh	|28000
Rajesh	|28000	|Shyamu	|28000
Ramesh	|25000	|Mahesh	|25000
*/

--LET'S GET ONLY SINGLE COLUMN--
SELECT A.NAME AS A_NAME, A.SALARY AS A_SAL FROM testdb.EMPLOYEE A INNER JOIN testdb.EMPLOYEE B
ON A.SALARY = B.SALARY
WHERE A.NAME<> B.NAME;
/*
A_NAME	|A_SAL
Mahesh	|25000
Shyamu	|28000
Rajesh	|28000
Ramesh	|25000
*/

---------------------------------------------------------------------------------
--------------------------GET THE MANAGER NAME OF EMPLOYEE ----------------------
SELECT * FROM testdb.EMPLOYEE;
/* LAST COLUMN IS MANAGER ID
1	|Ramesh	|GNoida	|9855498465	|25000	|101	|
2	|Suresh	|GNoida	|9856549846	|75000	|102	|1
3	|Rajesh	|GNoida	|9855497865	|28000	|103	|2
4	|Shyamu	|BSB	|9853698465	|28000	|102	|3
5	|Ramu	|BSB	|9855498235	|96000	|101	|2
6	|Mahesh	|GNoida	|9851678465	|25000	|101	|1
7	|Chaman	|BSBS	|9856723465	|215000	|103	|3
*/

SELECT A.EMP_ID AS A_ID, B.NAME AS EMP_NAME, 
A.NAME AS MANAGER_NAME, B.MANAGER_ID AS B_MID
FROM testdb.EMPLOYEE A INNER JOIN testdb.EMPLOYEE B
ON A.EMP_ID = B.MANAGER_ID
AND A.EMP_ID <> B.EMP_ID

/*
EMP_ID|EMP_NAME|MANAGER_NAME|MANAGER_ID
2	Suresh	Ramesh	1
3	Rajesh	Suresh	2
4	Shyamu	Rajesh	3
5	Ramu	Suresh	2
6	Mahesh	Ramesh	1
7	Chaman	Rajesh	3
*/

