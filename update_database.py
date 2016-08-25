from create_database import *
from connection import prompt_yes_no
import sys

class InvalidUpdateFileError(Exception):
    pass

class MrspecDatabaseUpdator(MrspecDatabaseEditor):
    
    def standardized_table_from_update_file(self, update_file, standard_name):
        name = update_file.split('.')[0]
        name_merged = name + '_merged'
        standard_name='standard_' + name_merged
        
        self.import_csv(update_file, name, "text")        
        
        self.check_for_table_before_executing(name_merged, "CREATE TABLE IF NOT EXISTS {} SELECT * FROM {} LEFT JOIN OUTCOMES_GROUPED ON OUTCOMES_GROUPED.`MRN (column to be removed once study is in analysis phase)` = updates.HSC_Number AND str_to_date(OUTCOMES_GROUPED.Date, '%d/%m/%Y') = str_to_date(updates.ProcedureDate, '%d/%m/%Y')".format(name_merged, name))
        self.con.commit()
        
        self.cur.execute('alter table {} add column AgeAtScan bigint(21)'.format(name_merged)) if not self.column_exists(name_merged,'AgeAtScan') else None
        self.cur.execute("UPDATE {} as T SET T.AgeAtScan = (TO_DAYS(STR_TO_DATE(T.ProcedureDate,'%d/%m/%Y')) - TO_DAYS(STR_TO_DATE(T.PatientBirthDay,'%d/%m/%Y'))) where T.Scan_ID = Scan_ID".format(name_merged))
        self.con.commit()
        self.create_standardized_table(standard_name, name_merged, self.update_table_schema, None, 'Scan_ID')        
    
    def insert_new_scans(self, scan_file):
        #assume scans are unique
        name = scan_file.split('.')[0]
        name_merged = name + '_merged'
        standard_name='standard_' + name_merged
        
        self.drop_table_if_exists(name)
        self.drop_table_if_exists(name_merged)
        self.drop_table_if_exists(standard_name)
                                  
        self.import_csv(scan_file, name, "text")
        
        c,rows = self.execute_and_return_query("SELECT {0}.Scan_ID FROM {0},{1} WHERE {0}.Scan_ID={1}.Scan_ID GROUP BY Scan_ID".format(name,self.table))
        
        n = len(rows)
        if n > 0:
            raise InvalidUpdateFileError("No changes were made to '{}' because the following {} Scan_IDs in the scan file '{}' already exist in '{}': {}.".format(self.table,n,scan_file,self.table,', '.join([r[0] for r in rows])))
        
        self.standardized_table_from_update_file(scan_file, standard_name)
        
        self.insert_additional_metabolites(standard_name, self.met_to_calculate)
        self.insert_aggregate_metabolites_optimal(standard_name, self.met_to_calculate)
    
        ##rename Cr and Cho for 1.5T scans to tCr and tCho
        self.rename_lower_field_metabolites(standard_name)        
        
        self.create_null_sd_columns(standard_name)
        
        self.cur.execute('INSERT INTO {} SELECT * FROM {}'.format(self.table,standard_name))
        self.con.commit()
    
    def copy_column(self, dest_table, source_table, column, join_on,overwrite=False):
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
        
        self.drop_table_if_exists(name)
        self.import_csv(update_file, name, "varchar(500)")

        columns_for_import = ",".join(["CAST({0} AS {1}) AS {2}".format(c[0], c[1], c[0] if len(c) < 3 else c[2]) for c in self.outcomes_schema])
        column_names = [c[0] if len(c) < 3 else c[2] for c in self.outcomes_schema]
        set_statement = ','.join(['t.{0}=q.{0}'.format(n) for n in column_names])

        self.cur.execute("UPDATE {} as t,(SELECT {},{}.tabPatient_ID,DOB,`MRN (column to be removed once study is in analysis phase)` as ID,str_to_date(Date,'%d/%m/%Y') as cast_date FROM {} LEFT JOIN tab_mrn ON `MRN (column to be removed once study is in analysis phase)`=HSC_Number GROUP BY ID,cast_date ORDER BY cast_date) as q SET {} WHERE t.{}=q.{} AND t.AgeAtScan=(TO_DAYS(q.cast_date)-TO_DAYS(str_to_date(q.DOB, '%d/%m/%Y')))".format(self.table,columns_for_import,name,name,set_statement,self.unique_desc,self.unique_desc))
        self.con.commit()
        
    def update_database_ID(self, table):
        pass
        '''if not d.column_exists(table, 'DatabaseID'):
            cur.execute("alter table {} add column DatabaseID BIGINT(21)".format(table))
        for result in cur.execute("set @i=0;update {0} as t inner join (select ID,Scan_ID,@i:=@i+1 as num from (select ID,Scan_ID from {0} group by ID order by Scan_ID) t2) t1 on t.ID=t1.ID set DatabaseID=num;".format(table),multi=True): pass
        con.commit()'''

if __name__== "__main__":
    with MrspecDatabaseUpdator() as (d,con,cur):
        
        update = 'standard'
        
        d.insert_new_scans('updates.csv')     
        
        d.update_outcomes('outcomes_latest.csv')
        
        d.insert_additional_metabolites(update,d.met_to_calculate)
        
        d.remove_asterisks_blanks(d.table)
        
        #if prompt_yes_no("\nDo you wish to create/overwrite the windowed SD columns with null values? WARNING: This cannot be undone, and will take a long time to restore the values."): d.create_null_sd_columns(update)
        if prompt_yes_no("\nDo you wish to (re)calculate the windowed SD columns? WARNING: This will overwrite previous data, and will take a long time to restore the values."): d.populate_SD_table_without_multi('', '', '', False, True)
        