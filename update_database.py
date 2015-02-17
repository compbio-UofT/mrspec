from connection import *
from create_database import *

con,cur=establish_connection(sys.argv)

def update_database_ID(table):
    if not column_exists('standard', 'DatabaseID'):
        cur.execute("alter table {} add column DatabaseID BIGINT(21)".format(table))
    for result in cur.execute("set @i=0;update {0} as t inner join (select ID,Scan_ID,@i:=@i+1 as num from (select ID,Scan_ID from {0} group by ID order by Scan_ID) t2) t1 on t.ID=t1.ID set DatabaseID=num;".format(table),multi=True): pass
    con.commit()


if __name__== "__main__":
    
    #update database with deidentified column IDs
    update_database_ID('standard')
    con.close()