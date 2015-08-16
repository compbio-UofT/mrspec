from create_database import *
from connection import prompt_yes_no
import sys

class MrpecDatabaseUpdator(MrspecDatabaseEditor):
    
    def add_column(self, dest_table, source_table, column, join_on,overwrite=False):
        name = column[2] if len(column)==3 else column[0]
        join = join_on[2] if len(join_on)==3 else join_on[0]
        
        if not self.column_exists(dest_table, name):
            self.cur.execute("ALTER TABLE {} ADD COLUMN {} {}".format(dest_table,name,column[1]))
            self.con.commit()
        if not overwrite:
            if not self.silent: print("Column '{}' in table '{}' already exists, no changes made.".format(name,dest_table))
        else:
            self.cur.execute("UPDATE {} as S, {} as T SET S.{}=CAST(T.{} as {}) WHERE S.{}=T.{}".format(dest_table,source_table,name,column[0],column[1],join,join_on[0]))
            self.con.commit()
            if not self.silent: print("Column '{}' in table '{}' successfully added.".format(name,dest_table))
    
    def update_outcomes(self, update_file):
        
        name='outcomes_update'
        
        if self.table_exists(name):
            self.cur.execute("DROP TABLE {}".format(name))
            if not self.silent: print("Table '{}' dropped from database.".format(name))
        self.import_csv(update_file, name, "varchar(500)")

        columns_for_import = ",".join(["CAST({0} AS {1}) AS {2}".format(c[0], c[1], c[0] if len(c) < 3 else c[2]) for c in self.outcomes_schema])
        column_names = [c[0] if len(c) < 3 else c[2] for c in self.outcomes_schema]
        set_statement = ','.join(['t.{0}=q.{0}'.format(n) for n in column_names])

        self.cur.execute("UPDATE {} as t,(SELECT {},DOB,`MRN (column to be removed once study is in analysis phase)` as ID,str_to_date(Date, '%d/%m/%Y') as cast_date FROM {} GROUP BY ID,cast_date ORDER BY cast_date) as q SET {} WHERE t.ID=q.ID AND t.AgeAtScan=(TO_DAYS(q.cast_date)-TO_DAYS(str_to_date(q.DOB, '%d/%m/%Y')))".format(self.table,columns_for_import,name,set_statement))
        self.con.commit()
        
    def update_database_ID(self, table):
        if not d.column_exists(table, 'DatabaseID'):
            cur.execute("alter table {} add column DatabaseID BIGINT(21)".format(table))
        for result in cur.execute("set @i=0;update {0} as t inner join (select ID,Scan_ID,@i:=@i+1 as num from (select ID,Scan_ID from {0} group by ID order by Scan_ID) t2) t1 on t.ID=t1.ID set DatabaseID=num;".format(table),multi=True): pass
        con.commit()
        
    def rename_lower_field_metabolites(self):
        if self.execute_and_return_query("select count(Cr) from standard where ScanBZero=1.5")[1][0][0]:
            cur.execute("UPDATE standard SET tCr=Cr,tCho=Cho,`tCr_%SD`=`Cr_%SD`,`tCho_%SD`=`Cho_%SD` WHERE ScanBZero=1.5 AND standard.Scan_ID=Scan_ID")
            cur.execute("UPDATE standard SET tCr_opt=Cr WHERE (`tCr_%SD` BETWEEN 0.000001 AND {}) AND ScanBZero=1.5 AND standard.Scan_ID=Scan_ID".format(d.met_threshold['tCr_opt']))
            cur.execute("UPDATE standard SET tCho_opt=Cho WHERE (`tCho_%SD` BETWEEN 0.000001 AND {}) AND ScanBZero=1.5 AND standard.Scan_ID=Scan_ID".format(d.met_threshold['tCho_opt']))
            con.commit()
            cur.execute("UPDATE standard SET Cr=NULL,Cho=NULL,`Cr_%SD`=NULL,`Cho_%SD`=NULL WHERE ScanBZero=1.5")
            con.commit()
            if not self.silent: print("\nRenamed Cr and Cho from 1.5T to tCr and tCho (and tCr_opt,tCho_opt)")            

if __name__== "__main__":
    with MrpecDatabaseUpdator() as (d,con,cur):
        
        update = 'standard'

        #calculate additional metabolites
        #d.insert_additional_metabolites(update, d.met_to_calculate)
        #d.insert_aggregate_metabolites_optimal(update, d.met_to_calculate)
        
        ##rename Cr and Cho for 1.5T scans to tCr and tCho
        d.rename_lower_field_metabolites()        
        
        d.update_outcomes('outcomes3.csv')
        
        #if prompt_yes_no("\nDo you wish to create/overwrite the windowed SD columns with null values? WARNING: This cannot be undone, and will take a long time to restore the values."): d.create_null_sd_columns(update)
        if prompt_yes_no("\nDo you wish to (re)calculate the windowed SD columns? WARNING: This will overwrite previous data, and will take a long time to restore the values."): d.populate_SD_table_without_multi('', '', '', False, True)
        