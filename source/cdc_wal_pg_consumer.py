## Consumer for Postgres CDC WAL (Change Data Capture using Write Ahead Log)

## WAL Message Format:
## https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

import struct
import psycopg2
from psycopg2.extras import LogicalReplicationConnection


# Database connection parameters
conn_params = {
    "dbname": "db_name",
    "user": "db_user",
    "password": "db_password",
    "host": "localhost",
    "port": "5432",
    "connection_factory": LogicalReplicationConnection
}


# --- HELPER: Decoding Null-Terminated Strings ---
def read_string(data, offset):
    end = data.find(b'\x00', offset)
    return data[offset:end].decode('utf-8'), end + 1


# --- THE DECODER ---
class PGOutputDecoder:
    def __init__(self):
        self.relations = {}  # Cache table metadata

    def get_relation_characteristics(self, payload, offset): # replica identity  + nr of columns
        '''
        Parses the replica identity (none or full) and nr of columns for relation
        '''
        rel_details = {}
        cols = []
        # this replica identity is a single value so:
        repl_ident = payload[offset:offset+1].decode()
        offset += 1
        nr_of_cols = struct.unpack('>H', payload[offset:offset+2])[0]
        offset += 2
        rel_details = {"repl_ident": repl_ident, "nr_of_cols": nr_of_cols}
        #print(f'get_relation_characteristics, offset: {offset} repl_ident: {repl_ident}, nr_of_cols: {nr_of_cols}' )
        #print(f'get_relation_characteristics, rel_details: {rel_details}' )
        # now we parse the columns and their characteristics
        for _ in range(nr_of_cols):
            # just before the name we got 1 byte - the col flag (0 for nothing and 1 if col is part of the key)
            # oddly enough in a truncate all columns seem to be part of the key ???!!!
            indiv_col = {}
            is_key_flag = struct.unpack('>B', payload[offset:offset+1])[0]
            col_name, offset = read_string(payload, offset+1)
            indiv_col = {"col_name":col_name, "is_key_flag":is_key_flag}
            #print(f'col_name: {col_name}, is_key_flag: {is_key_flag}, offset: {offset}' )
            col_type_oid = struct.unpack('>I', payload[offset:offset+4])[0] # oid of column data type in pg_type
            offset += 4
            indiv_col.update({"col_type_oid":col_type_oid})
            #print(f'col_oid: {col_type_oid}, offset: {offset}' )
            col_atttypmod = struct.unpack('>i', payload[offset:offset+4])[0] # column modifier (in pg_attribute) - generally -1, no modifier
            offset += 4
            indiv_col.update({"col_atttypmod":col_atttypmod})
            #print(f'col_atttypmod: {col_atttypmod}, offset: {offset}' )
            cols.append(indiv_col)
            #print(f'cols: {cols}')

        rel_details.update({"cols":cols})
        #print(f'rel_details: {rel_details}')
        return rel_details

    def parse_tuple(self, payload, offset):
        """
        Parses a row of data. Returns the list of values and the new offset.
        Format: 2 bytes (col count), then for each col: 1 byte (type), 4 bytes (len), data.
        """
        n_cols = struct.unpack('>H', payload[offset:offset+2])[0]
        offset += 2
        columns = []

        for _ in range(n_cols):
            col_type = chr(payload[offset])
            offset += 1
            
            if col_type == 't':  # Text data
                length = struct.unpack('>I', payload[offset:offset+4])[0]
                offset += 4
                val = payload[offset:offset+length].decode('utf-8')
                columns.append(val)
                offset += length
            elif col_type == 'n':  # Null
                columns.append(None)
            elif col_type == 'u':  # Unchanged TOAST
                columns.append("[UNCHANGED_TOAST]")
        
        return columns, offset

    def decode(self, payload):

        # print(f'self.relations: {self.relations}')
        
        msg_type = chr(payload[0])
        # print(f'in decode, payload: {payload}, msg_type chr(paylod[0]): {msg_type} ')

        # --- Transaction Boundaries ---
        if msg_type == 'B':  # BEGIN
            print(f"[{msg_type}] Transaction Started")
            
        elif msg_type == 'C':  # COMMIT
            print(f"[{msg_type}] Transaction Committed")

        # --- Metadata ---
        elif msg_type == 'R':  # RELATION
            rel_id = struct.unpack('>I', payload[1:5])[0]
            # print(f'struct.unpack(\'>I\', payload[1:5])[0]: {struct.unpack('>I', payload[1:5])[0]}')
            # print(f'payload: {payload}')

            namespace, offset = read_string(payload, 5)
            # print(f'namespace: {namespace}, offset: {offset}')

            name, offset = read_string(payload, offset)
            # print(f'name: {name}, offset: {offset}')

            # Relation / Table characteristics like repl ident and nr of cols
            # Replica Indentity (relreplident in pg_class) (SQL: ALTER TABLE table_cdc_wal_1 REPLICA IDENTITY FULL;)
            rel_details = self.get_relation_characteristics(payload, offset)
    
            self.relations[rel_id] = {'name': name, 'schema': namespace, 'rel_details': rel_details}
            # print(f'self.relations: {self.relations} ')
            print(f"[{msg_type}] Relation Mapping: {namespace}.{name} (ID: {rel_id})")

        # --- INSERT ---
        elif msg_type == 'I':
            rel_id = struct.unpack('>I', payload[1:5])[0]
            # print(f'Insert, rel_id: {rel_id}')

            rel = self.relations.get(rel_id, {"name": "unknown"})
            # print(f'Insert, rel: {rel}')

            # Offset 5 is 'N' (New Tuple), data starts at offset 6
            values, _ = self.parse_tuple(payload, 6)
            # print(f'Insert, values: {values}')

            print(f"[{msg_type}] INSERT in {rel['name']}: {values}")

        # --- UPDATE ---
        elif msg_type == 'U':
            rel_id = struct.unpack('>I', payload[1:5])[0]
            rel = self.relations.get(rel_id, {"name": "unknown"})
            offset = 5
            
            # Update can have 'K' (Key) or 'O' (Old) before the 'N' (New)
            prefix = chr(payload[offset])
            if prefix in ('K', 'O'):
                # We parse the old data but just move the offset forward
                _, offset = self.parse_tuple(payload, offset + 1)
            
            if chr(payload[offset]) == 'N':
                new_values, _ = self.parse_tuple(payload, offset + 1)
                print(f"[{msg_type}] UPDATE in {rel['name']} | New Data: {new_values}")

        # --- DELETE ---
        elif msg_type == 'D':
            rel_id = struct.unpack('>I', payload[1:5])[0]
            rel = self.relations.get(rel_id, {"name": "unknown"})
            offset = 5
            
            # Delete has either 'K' (Key) or 'O' (Old)
            prefix = chr(payload[offset])
            deleted_values, _ = self.parse_tuple(payload, offset + 1)
            label = "Primary Key" if prefix == 'K' else "Full Row"
            print(f"[{msg_type}] DELETE in {rel['name']} | {label}: {deleted_values}")

        # --- TRUNCATE ---
        elif msg_type == 'T':
            rel_id = struct.unpack('>I', payload[6:10])[0]
            rel = self.relations.get(rel_id, {"name": "unknown"})
            print(f"[{msg_type}] Truncate {rel['name']}")
            #print(f'truncate payload: {payload}')

# --- INTEGRATION WITH CONSUMER ---
decoder = PGOutputDecoder()

def process_message(msg):

    if not msg.payload:
        msg.cursor.send_feedback(write_lsn=msg.data_start)
        return
    
    try:
        # print(f'right before calling decoder, msg.data_start: {msg.data_start}') # data_start is the LSN
        decoder.decode(msg.payload)
    except Exception as e:
        print(f"Decoding Error: {e}")

    msg.cursor.send_feedback(
        write_lsn=msg.data_start, 
        flush_lsn=msg.data_start, 
        apply_lsn=msg.data_start
    )

try:
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    
    ## The code below is after changing the decoding from binary to test_decoding so we can deal with test instead
    ## of binary which is what pgoutput does
    options = {'proto_version': '2', 'publication_names': 'ms_cdc_publication'}

    print("Starting CDC Stream ...")
    cur.start_replication(
        slot_name='ms_cdc_slot',
        options=options,
        decode=False # Returns the raw bytes for you to decode
    )

    while True:
        try:
            cur.consume_stream(process_message)
        except Exception as gen_exception:
            print(f'Exception {gen_exception} this script should restart within its while True loop')


except KeyboardInterrupt:
    cur.close()
    conn.close()
    print("Stream stopped.")

