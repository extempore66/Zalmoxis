### Postgres CDC by monitoring and parsing the WAL

## Description
This is a fully functional utility which monitors and parses Postgres' Write Ahead Log for DML changes.
The tables (relations as they're called in SQL and WAL parlance) whose changes are recorded pass their composition
(nr of columns, their data types, whether they are part of the transaction key, etc) so DDL changes are reflected in real time as well, enabling leveraging those changes downstream to data storage targets.

## Getting Started
You need Postgres installed - any version above and including 15 is recommended.
Any database IDE like PGAdmin, DBeaver, DBArtisan is also needed.



