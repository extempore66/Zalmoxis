## Postgres CDC by monitoring and parsing the WAL

This is a fully functional utility which monitors and parses Postgres' Write Ahead Log for DML and changes to the database
The tables (relations as they're called in SQL and WAL parlance) whose changes are recorded pass their composition
(nr of columns, their data types, whether they are part of the transaction key, etc) each time so if there are DML changes 
from a previous transaction, the table's new definition is passed in the new transaction.

