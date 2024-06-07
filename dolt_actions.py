import pandas as pd
from sqlalchemy import (
    create_engine,
    text,
    Table,
    select,
    insert,
    and_,
    MetaData
)
from pprint import pprint

def reset_database(engine):
    metadata_obj = MetaData()

    # Here we find the first commit in the log and reset to that commit
    dolt_log = Table("dolt_log", metadata_obj, autoload_with=engine)
    stmt = select(dolt_log.c.commit_hash).limit(1).order_by(dolt_log.c.date.asc())
    with engine.connect() as conn:
        results_obj = conn.execute(stmt)
        results = results_obj.fetchall()
        init_commit_hash = results[0][0]

        dolt_reset_hard(engine, init_commit_hash)

def reset_database_head(engine):
    metadata_obj = MetaData()

    # Here we find the first commit in the log and reset to that commit
    dolt_log = Table("dolt_log", metadata_obj, autoload_with=engine)
    stmt = select(dolt_log.c.commit_hash).limit(2).order_by(dolt_log.c.date.desc())
    with engine.connect() as conn:
        results_obj = conn.execute(stmt)
        results = results_obj.fetchall()
        init_commit_hash = results[0][0]

        dolt_reset_hard(engine, init_commit_hash)

def delete_non_main_branches(engine):
    metadata_obj = MetaData()

    # Iterate through the non-main branches and delete them with
    # CALL DOLT_BRANCH('-D', '<branch>'). '-D' force deletes just in
    # case I have some unmerged modifications from a failed run.
    dolt_branches = Table("dolt_branches", metadata_obj, autoload_with=engine)
    stmt = select(dolt_branches.c.name).where(and_(dolt_branches.c.name != 'main', dolt_branches.c.name != 'master'))
    with engine.connect() as conn:
        results = conn.execute(stmt)
        for row in results:
            branch = row[0];
            print("Deleting branch: " + branch)
            stmt = text("CALL DOLT_BRANCH('-D', '" + branch + "')")
            conn.execute(stmt)

'''
Retrieves table with given table name.
'''
def load_table(engine, table_name):
    metadata_obj = MetaData()
    return Table(table_name, metadata_obj, autoload_with=engine)
 
'''
Insert given data to given table name. The data is in format of array of dictionary.
'''
def insert_data(engine, table_name, data):
    table = load_table(engine, table_name)
    
    # This is standard SQLAlchemy
    stmt = insert(table).values(data)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

def drop_table(engine, table_name):
    if not engine.dialect.has_table(engine, table_name):
        print("Table not found: "+ table)
    else:
        table = load_table(engine, table_name)
        table.drop(engine)
        
def dolt_commit(engine, author, message):
    # Dolt exposes version control writes as procedures
    # Here, we use text to execute procedures.
    #
    # The other option is to do something like:
    #
    # conn = engine.raw_connection()
    # results = conn.cursor().callproc('dolt_commit', arguments)
    # conn.close()
    #
    # I like the text approach better.
    with engine.connect() as conn:
        # -A means all tables
        conn.execute(
            text("CALL DOLT_ADD('-A')")
        )
        # --skip-empty so this does not fail if there is nothing to commit
        result = conn.execute(
            text("CALL DOLT_COMMIT('--skip-empty', '--author', '"
                 + author
                 + "', '-m', '"
                 + message
                 + "')")
        )
        commit = None
        # Dolt stored procedures return results
        for row in result:
            commit = row[0]
        if ( commit ): 
            print("Created commit: " + commit )

def dolt_reset_hard(engine, commit):
    if ( commit ):
        stmt = text("CALL DOLT_RESET('--hard', '" + commit + "')")
        print("Resetting to commit: " + commit)
    else:
        stmt = text("CALL DOLT_RESET('--hard')")
        print("Resetting to HEAD")

    with engine.connect() as conn:
        results = conn.execute(stmt)
        conn.commit()

def dolt_create_branch(engine, branch):
    # Check if branch exists
    metadata_obj = MetaData()

    dolt_branches = Table("dolt_branches", metadata_obj, autoload_with=engine)
    stmt = select(dolt_branches.c.name).where(dolt_branches.c.name == branch)
    with engine.connect() as conn:
        results = conn.execute(stmt)
        rows = results.fetchall()
        if ( len(rows) > 0 ):
             print("Branch exists: " + branch)
             return

    # Create branch
    stmt = text("CALL DOLT_BRANCH('" + branch + "')")
    with engine.connect() as conn:
        results = conn.execute(stmt)
        print("Created branch: " + branch)

def dolt_checkout(db, branch):
    engine_base = "mysql+mysqlconnector://root@127.0.0.1:3306/" + db
    # Branches can be "checked out" via connection string. We make heavy use
    # of reflection in this example for system tables so passing around an
    # engine instead of a connection is best for this example. 
    engine = create_engine(
    	engine_base + "/" + branch
    )
    print("Using branch: " + branch)
    return engine

def dolt_merge(engine, branch):
    stmt = text("CALL DOLT_MERGE('" + branch + "')")
    with engine.connect() as conn:
        results = conn.execute(stmt)
        rows = results.fetchall()
        commit       = rows[0][0]
        fast_forward = rows[0][1]
        conflicts    = rows[0][2]
        print("Merge Complete: " + branch)
        print("\tCommit: " + commit)
        print("\tFast Forward: " + str(fast_forward))
        print("\tConflicts: " + str(conflicts))

def print_commit_log(engine):
    # Examine a dolt system table, dolt_log, using reflection
    metadata_obj = MetaData()
    print("Commit Log:")

    dolt_log = Table("dolt_log", metadata_obj, autoload_with=engine)
    stmt = select(dolt_log.c.commit_hash,
                  dolt_log.c.committer,
                  dolt_log.c.message
                  ).order_by(dolt_log.c.date.desc())

    with engine.connect() as conn:
        results = conn.execute(stmt)
        for row in results:
            commit_hash = row[0]
            author      = row[1]
            message     = row[2]
            print("\t" + commit_hash + ": " + message + " by " + author)

def print_status(engine):
    metadata_obj = MetaData()
    dolt_status = Table("dolt_status", metadata_obj, autoload_with=engine)

    print("Status")
    stmt = select(dolt_status.c.table_name, dolt_status.c.status)
    with engine.connect() as conn:
        results = conn.execute(stmt)
        rows = results.fetchall();
        if ( len(rows) > 0 ):
            for row in rows:
                table  = row[0]
                status = row[1]
                print("\t" + table + ": " + status)
        else:
            print("\tNo tables modified")

def print_active_branch(engine):
    stmt = text("select active_branch()")
    with engine.connect() as conn:
        results = conn.execute(stmt)
        rows = results.fetchall()
        active_branch = rows[0][0]
        print("Active branch: " + active_branch)
            
def print_diff(engine, table):
    metadata_obj = MetaData()

    print("Diffing table: " + table)
    dolt_diff = Table("dolt_diff_" + table,
                      metadata_obj,
                      autoload_with=engine)

    # Show only working set changes
    stmt = select(dolt_diff).where(dolt_diff.c.to_commit == 'WORKING')
    with engine.connect() as conn:
        results = conn.execute(stmt)
        for row in results:
            # I use a dictionary here because dolt_diff_<table> is a wide table
            row_dict = row._asdict()
            # Then I use pprint to display the results
            pprint(row_dict)
    
def print_tables(engine):
    # Raw SQL here to show what we've done
    with engine.connect() as conn:
        result = conn.execute(text("show tables"))

        print("Tables in database:")
        for row in result:
            table = row[0]
            print("\t" + table)
