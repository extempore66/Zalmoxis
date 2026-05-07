from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

    # Connection settings
    mysql_settings = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "cdc_user",
        "passwd": "your_password"
    }

    # The stream reader acts as a "virtual slave"
    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=100,               # Must be different from the MySQL server-id
        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
        blocking=True,               # Keep the connection open and wait for events
        resume_stream=True           # Start from the last known position
    )

    try:
        print("Starting MySQL CDC Stream...")
        for binlogevent in stream:
            # Each event can contain multiple rows (e.g., a bulk insert)
            for row in binlogevent.rows:
                event_data = {
                    "schema": binlogevent.schema,
                    "table": binlogevent.table,
                    "timestamp": binlogevent.timestamp,
                }

                if isinstance(binlogevent, WriteRowsEvent):
                    print(f"[INSERT] {event_data['table']}: {row['values']}")

                elif isinstance(binlogevent, UpdateRowsEvent):
                    print(f"[UPDATE] {event_data['table']}")
                    print(f"  - Before: {row['before_values']}")
                    print(f"  - After:  {row['after_values']}")

                elif isinstance(binlogevent, DeleteRowsEvent):
                    print(f"[DELETE] {event_data['table']}: {row['values']}")

    except KeyboardInterrupt:
        stream.close()
        print("Stream stopped.")
