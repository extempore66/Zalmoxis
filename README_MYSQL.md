## MySQL CDC by monitoring and parsing the Binlog

### Description
This is a fully functional utility which monitors and parses Postgres' Write Ahead Log for DML changes. The tables (relations as 
they're called in SQL and WAL parlance), whose DML changes are recorded, pass their composition (nr of columns, their data types, whether 
they are part of the transaction key, etc) so DDL changes are reflected in real time as well, enabling leveraging those changes 
downstream to data storage targets.
For management of schema evolution selecting the table / column information from information_schema.tables in conjunction with 
information_schema.columns system views or pg_class, pg_attribute, pg_type system tables with a certain frequency and comparing the results 
with the destination gives you a real-time, full view and control of the schemas and thus essentially you have built a perfectly sophisticated,
quasi automatic system (human intervention can be employed when needed) for hot standby replicas, backups, ETLs, etc.

### Prerequisites:
    1.  MySQL installed
    2.  Any database IDE like PGAdmin, DBeaver, DBArtisan is also needed.
    3.  Python (version used here is 3.13.5)


### Getting Started
Phase 1: Configure MySQL (The Binlog)
    You’ll need to edit your configuration file (usually my.cnf, my.ini, or /etc/mysql/mysql.conf.d/mysqld.cnf).

    1. Modify the configuration:

        [mysqld]
        server-id         = 1                # Unique ID for this server
        log-bin           = mysql-bin        # Enable the binary log
        binlog_format     = ROW              # CRITICAL: Captures specific row changes
        binlog_row_image  = FULL             # Captures all columns (like REPLICA IDENTITY FULL)
        expire_logs_days  = 7                # Auto-delete logs after 7 days

    2. Restart MySQL to apply changes.

    3. Verify status:
    Run SHOW VARIABLES LIKE 'binlog_format';—it must return ROW.

Phase 2: Create a Replication User
    MySQL requires a user with specific "Slave" privileges to read the binlog stream.

    CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'your_password';

    -- Grant the ability to request the binlog and check server status
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

    -- Grant SELECT on the specific tables you want to track
    GRANT SELECT ON your_database.* TO 'cdc_user'@'%';

    FLUSH PRIVILEGES;


Phase 3: Concrete Implementation (Python)
    For MySQL, the gold standard is the python-mysql-replication library. It acts as a "virtual slave," connecting to the server and parsing 
    the binary events into Python dictionaries for you.

    Prerequisites: pip install mysql-replication


    


Crucial Maintenance Notes
1. The "Binlog Files" (Your Disk Space)
In Postgres, a "Slot" holds the logs until you consume them. In MySQL, the logs are rotated based on your expire_logs_days setting.

Warning: If your Python script is offline longer than your expire_logs_days, you will lose data. Unlike Postgres, MySQL won't "wait" for your consumer unless you have a true replica configured.

2. Resume Logic
To build a production system, you must save the Binlog File Name and the Log Position (found in binlogevent.packet.log_pos) to a database or file. If your script crashes, you restart the stream using those coordinates:

Python
stream = BinLogStreamReader(
    connection_settings=mysql_settings,
    server_id=100,
    log_file='mysql-bin.000001', # Saved from previous run
    log_pos=456,                  # Saved from previous run
    resume_stream=True
)
3. Server IDs
The server_id in your Python code must be unique. If you use the same ID as the MySQL server (usually 1) or another running consumer, MySQL will get confused and disconnect both.

Does the "Virtual Slave" approach for MySQL feel more or less complex than the "Logical Slot" approach we used for Postgres?
