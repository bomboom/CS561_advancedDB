B = LOAD '/user/hadoop/input/Transactions.csv' Using PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, Items:int, TransDesc:chararray);
groupB = Group B BY CustID;
result = FOREACH groupB GENERATE group, COUNT(B), SUM(B.TransTotal);
DUMP result;
STORE result INTO 'outputquery1.txt' USING PigStorage(',');
