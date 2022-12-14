-- SELECT * FROM EMPLOYEE
/*
EMP_ID	|NAME 	|ADDRESS| PHONE 	|SALARY |DEPT_ID
1		|Ramesh	|GNoida	|9855498465	|25000	|101
2		|Suresh	|GNoida	|9856549846	|75000	|102
3		|Rajesh	|GNoida	|9855497865	|28000	|103
4		|Shyamu	|BSB	|9853698465	|35000	|102
5		|Ramu	|BSB	|9855498235	|96000	|101
6		|Mahesh	|GNoida	|9851678465	|25000	|101
7		|Chaman	|BSBS	|9856723465	|215000	|103 
*/

-- SELECT * FROM DEPARTMENT
/* 
DEPT_ID | DEPT_NAME
101		| HR
102		| SOFTWARE
103		| SALES 
*/

-- GET DATA BY INNER JOIN 
SELECT E.EMP_ID, E.NAME, E.SALARY, D.DEPT_ID, D.DEPT_NAME FROM testdb.employee E INNER JOIN 
testdb.department D ON E.DEPT_ID=D.DEPT_ID;
/*
EMP_ID| NAME| 	SALARY| DEPT_ID| DEPT_NAME
1| 		Ramesh| 25000|  101| 	 HR
2| 		Suresh| 75000|  102| 	 SOFTWARE
3| 		Rajesh| 28000|  103| 	 SALES
4| 		Shyamu| 35000|  102| 	 SOFTWARE
5| 		Ramu| 	96000|  101| 	 HR
6| 		Mahesh| 25000|  101| 	 HR
7| 		Chaman| 215000| 103| 	 SALES 
*/

-- GET MAX SALRARY OF EMP FROM DEPARTMENT SOFTWARE
SELECT E.EMP_ID, E.NAME, E.SALARY, D.DEPT_ID, D.DEPT_NAME, MAX(E.SALARY) AS MAX_SALARY FROM testdb.employee E INNER JOIN 
testdb.department D ON E.DEPT_ID=D.DEPT_ID
WHERE D.DEPT_NAME='SOFTWARE';

/* OUTPUT - 
EMP_ID| NAME	| SALARY| DEPT_ID| DEPT_NAME| MAX_SALARY
2	  | Suresh	| 75000 | 102	 | SOFTWARE | 75000 
*/
