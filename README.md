## Postgres CDC by monitoring and parsing the WAL

### Description
This is a fully functional utility which monitors and parses Postgres' Write Ahead Log for DML changes. The tables (relations as 
they're called in SQL and WAL parlance), whose changes are recorded, pass their composition (nr of columns, their data types, whether 
they are part of the transaction key, etc) so DDL changes are reflected in real time as well, enabling leveraging those changes 
downstream to data storage targets.

### Prerequisites:
    1.  Postgres installed - any version above and including 10 is recommended (version used here is 17). 
    2.  Any database IDE like PGAdmin, DBeaver, DBArtisan is also needed.
    3.  Python (version used here is 3.13.5)


### Getting Started
    1.  Enable WAL (specifically Logical Replication ) in your Postgres postgresql.conf file
        
        Locate your postgresql.conf file. The best way is to issue the following command in your database IDE:
            show data_directory;
       
        In postgresql.conf locate and modify the following parameters (they will require restart of Postgres):
            wal_level = logical
            max_wal_senders = 5 			# (for development values are less important, go with 5)
            max_replication_slots = 5       # (for development values are less important, go with 5)

    2. In your database IDE 
    
        Set permissions for the user:
            ALTER ROLE <your_db_user> WITH REPLICATION;

        Create some tables:
            CREATE TABLE table_cdc_wal_1 (
                col_cdc_wal_1 int4 NOT NULL,
                col_cdc_wal_2 varchar(10) NULL,
                col_cdc_wal_jsonb_3 jsonb NULL,
                CONSTRAINT table_cdc_wal_1_pkey PRIMARY KEY (col_cdc_wal_1)
            );

            CREATE TABLE table_cdc_wal_2 (
                col_cdc_wal_1 int4 NOT NULL,
                col_cdc_wal_2 varchar(10) NULL,
                col_cdc_wal_jsonb_3 jsonb NULL,
                CONSTRAINT table_cdc_wal_2_pkey PRIMARY KEY (col_cdc_wal_1)
            );

        Create a publication for the tables above:
            CREATE PUBLICATION ms_cdc_publication FOR TABLE table_cdc_wal_1, table_cdc_wal_2;
        
                (alternatively you can create publication for all tables):
                    CREATE PUBLICATION ms_cdc_publication FOR ALL TABLES;

                (if you want to see the publications created):
                    select * from pg_publication;

        Create a Logical Replication Slot. A Replication Slot ensures that the WAL files are not deleted by Postgres
        until they have been processed by your consumer. Using the 'pgoutput' plugin (standard since Postgres 10):
            SELECT * FROM pg_create_logical_replication_slot('ms_cdc_slot', 'pgoutput');


        If you want to see the actual values of the changes reflected in the monitoring script, (this tells Postgres to 
        include the old values of all columns in the WAL):
            ALTER TABLE table_cdc_wal_1 REPLICA IDENTITY FULL;
            ALTER TABLE table_cdc_wal_2 REPLICA IDENTITY FULL;

                (alternatively , for more space saving)
                ALTER TABLE table_cdc_wal_1 REPLICA IDENTITY DEFAULT;
                ALTER TABLE table_cdc_wal_2 REPLICA IDENTITY DEFAULT;

        ------  YOU ARE DONE WITH DATABASE PREPS -------
    
    3. Setup Pythonn environment and import libraries

        Create a directory, switch to it, setup a Python virtual environment and activate it (these are for 
        MacOS / Linux):
            mkdir cdc_test
            cd cdc_test
            python -m venv .venv
            source .venv/bin/activate 

        Install the following libraries for Python:
            pip struct
            pip psycopg2

        Download file cdc_wal_pg_consumer_raw.py from the source folder in this repository into cdc_test folder 
        created at previous step on your machine and run it:
            python cdc_wal_pg_consumer_raw.py 

        You should see a confirmation it is running:
            Starting CDC Stream ...



    5.  Executing some DML on your Postgres to see the changes captured from the WAL and reflected in the script:
        ( Deliberately using complex data types like jsonb to show the versatility of WAL )

            Some inserts to populate the tables:
                insert into table_cdc_wal_1 (col_cdc_wal_1, col_cdc_wal_2, col_cdc_wal_jsonb_3) values('1', 'desc val 1',
                '[{"json_obj_1_int_id":1,"json_obj_1_str_id":"json_obj_1_str_id_val","date_created_gmt":1720455091},
                {"json_obj_2_int_id":2,"json_obj_2_str_id":"json_obj_2_str_id_val","date_created_gmt":1720456091}]' );

                insert into table_cdc_wal_1 (col_cdc_wal_1, col_cdc_wal_2, col_cdc_wal_jsonb_3) values('2', 'desc val 2',
                '[{"json_obj_1_int_id":1,"json_obj_1_str_id":"json_obj_1_str_id_val","date_created_gmt":1720455091},
                {"json_obj_2_int_id":2,"json_obj_2_str_id":"json_obj_2_str_id_val","date_created_gmt":1720456091}]' );

                insert into table_cdc_wal_2 (col_cdc_wal_1, col_cdc_wal_2, col_cdc_wal_jsonb_3) values('10', 'descval 10',
                '[{"json_obj_1_int_id":1,"json_obj_1_str_id":"json_obj_1_str_id_val","date_created_gmt":1720455091},
                {"json_obj_2_int_id":2,"json_obj_2_str_id":"json_obj_2_str_id_val","date_created_gmt":1720456091}]' );

            In general Enter any SQL DML you wish (on those 2 tables) and see the script capture it in real time


            If you feel adveturous, uncomment this line in the code (around line 134) and restart the script:
                # print(f'self.relations: {self.relations} ')    

            Then enter some DDL:
                ALTER TABLE table_cdc_wal_1 ADD col_cdc_wal_4 BIGINT;
                ALTER TABLE table_cdc_wal_1 ADD col_cdc_wal_5 varchar(255);
                ALTER TABLE table_cdc_wal_2 ADD col_cdc_wal_4 BIGINT;
                ALTER TABLE table_cdc_wal_2 ADD col_cdc_wal_5 varchar(255);

            Enter some more DML commands and see the changes of the table columns reflected in the script:
                begin transaction;
                insert into  table_cdc_wal_2 (col_cdc_wal_1, col_cdc_wal_2, col_cdc_wal_4, col_cdc_wal_5)
                values(40, 'descval 40', 76543210, 'inserting entire row for pk 40 in table table_cdc_wal_2 as 
                part of a multi table transaction');
                insert into  table_cdc_wal_1 (col_cdc_wal_1, col_cdc_wal_2, col_cdc_wal_4, col_cdc_wal_5)
                values(4, 'descval 4', 76543210, 'inserting entire row for pk 4 in table table_cdc_wal_1 as part of a 
                multi table transaction');
                update table_cdc_wal_1 set col_cdc_wal_5 = 'updating this as part of a multi table transaction testing 
                the python cdc script' where col_cdc_wal_1 = '4';
                commit;

    ### Output
        If all goes well, you should see output similar to this:

            [B] Transaction Started
            [R] Relation Mapping: public.table_cdc_wal_2 (ID: 16447)
            [I] INSERT in table_cdc_wal_2: ['50', 'descval 50', None, '11111222', 'inserting entire row for pk 50 in table table_cdc_wal_2 as 
                part of a multi table transaction']
            [R] Relation Mapping: public.table_cdc_wal_1 (ID: 16440)
            [I] INSERT in table_cdc_wal_1: ['5', 'descval 5', None, '10101010', 'inserting entire row for pk 5 in table table_cdc_wal_1 as 
                part of a multi table transaction']
            [U] UPDATE in table_cdc_wal_1 | New Data: ['5', 'descval 5', None, '10101010', 'updating this as part of a multi table transaction testing                     the python cdc script']
            [C] Transaction Committed
    
