import pandas as pd
from dolt_actions import (
    dolt_checkout,
    print_active_branch, 
    dolt_create_branch, 
    dolt_commit,
    delete_non_main_branches,
    dolt_reset_hard,
)
from sqlalchemy import (create_engine, text, MetaData, Table, insert)

def main():
    database='stocks'
    table_name='dividend'

    engine = dolt_checkout(database,'master')
    print_active_branch(engine)

    # used to re-run the demo script
    # dolt_reset_hard(engine, 'HEAD~')
    # delete_non_main_branches(engine)

    # add rows with NULL value
    add_null_data(engine)
    dolt_commit(engine, 'Jennifer <jennifer@dolthub.com>', 'Insert values with NULL amount')

    # import data from the database as dataframe
    dividend = pd.read_sql_table(table_name=table_name, con=engine.connect())
    print('The first 5 entries: \n', dividend.head())
    print('All entries with NULL amount: \n', dividend[dividend['amount'].isna()])
    # The first 5 entries: 
    #    act_symbol    ex_date   amount
    # 0          A 2006-11-01  1.47139
    # 1          A 2006-11-02      NaN
    # 2          A 2012-03-30  0.07153
    # 3          A 2012-03-31      NaN
    # 4          A 2012-06-29  0.07153
    # All entries with NULL amount: 
    #    act_symbol    ex_date  amount
    # 1          A 2006-11-02     NaN
    # 3          A 2012-03-31     NaN

    # check out a dev branch
    dolt_create_branch(engine, 'dev-branch')
    engine = dolt_checkout(database,'dev-branch')
    print_active_branch(engine)

    # add insert with null statement? and commit?
    # updates the null value
    updated_dividend = dividend.ffill()

    update_db_table_with_df(engine, table_name, updated_dividend)

    diff = get_diff(engine, table_name, dividend.columns)
    print('The diff result: \n', diff)

'''
Inserts values to `dividend` table using SQLAlchemy. The `amount` values are NULL.
'''
def add_null_data(engine):
    metadata_obj = MetaData()
    dividend_table = Table("dividend", metadata_obj, autoload_with=engine)
    stmt = insert(dividend_table).values([
        {'act_symbol': 'A', 'ex_date': '2006-11-02', 'amount': None},
        {'act_symbol': 'A', 'ex_date': '2012-03-31', 'amount': None}, 
        ])
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

'''
Truncates the given table and inserts the given dataframe
'''
def update_db_table_with_df(engine, table_name, df):
    with engine.connect() as con:
        con.execute(text('TRUNCATE '+table_name+';'))
        con.commit()
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    
'''
Gets diff on given table in format of `from` to `to`.
'''
def get_diff(engine, table_name, columns):
    to_cols=''
    from_cols=''
    for col in columns:
        to_cols += ', to_'+col
        from_cols += ', from_'+col

    q = "select diff_type" + from_cols + to_cols + " from dolt_diff_" + table_name +" where to_commit='WORKING';"
    return pd.read_sql_query(q, con=engine.connect())
    
main()
