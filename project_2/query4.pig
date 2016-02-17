B = LOAD '/user/hadoop/input/Transactions.csv' Using PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, Items:int, TransDesc:chararray);
A = LOAD 'input/Customers.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Age:int, CCode:int, Salary:float);

proj_A = FOREACH A GENERATE ID, Name;
proj_B = FOREACH B GENERATE TransID, CustID;

C = JOIN proj_B BY CustID, proj_A by ID Using 'replicated';
D = GROUP C BY (ID, Name);
E = FOREACH D GENERATE FLATTEN(group) AS (ID, Name), COUNT(C) AS num;

re = GROUP E ALL;
min = FOREACH re GENERATE MIN(E.num) AS val;
with_min = FILTER E BY num == (long)min.val;
fil = GROUP with_min BY ID;
with_min = FOREACH fil GENERATE FLATTEN(with_min.Name), min.val;

/*
with_min = FOREACH re {
	ord = ORDER E BY num ASC;
	top = LIMIT ord 1;
 	GENERATE FLATTEN(top);
};
*/

DUMP with_min;
STORE with_min INTO 'outputquery4.txt' Using PigStorage(',');
