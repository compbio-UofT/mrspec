from connection import *
from create_database import *
import sys

def update_database_ID(table):
    if not column_exists(table, 'DatabaseID'):
        cur.execute("alter table {} add column DatabaseID BIGINT(21)".format(table))
    for result in cur.execute("set @i=0;update {0} as t inner join (select ID,Scan_ID,@i:=@i+1 as num from (select ID,Scan_ID from {0} group by ID order by Scan_ID) t2) t1 on t.ID=t1.ID set DatabaseID=num;".format(table),multi=True): pass
    con.commit()


if __name__== "__main__":
    with DatabaseConnection(sys.argv) as (con,cur):
        #insert_additional_metabolites('standard_sd', met_to_calculate)
        insert_aggregate_metabolites_optimal('standard_sd', met_to_calculate)
        
        #update database with deidentified column IDs
        ##update_database_ID('standard')