from create_database import *
import sys

class MrpecDatabaseUpdator(MrspecDatabaseEditor):
    
    def update_outcomes(self, update_file):
        pass

    def update_database_ID(self, table):
        if not d.column_exists(table, 'DatabaseID'):
            cur.execute("alter table {} add column DatabaseID BIGINT(21)".format(table))
        for result in cur.execute("set @i=0;update {0} as t inner join (select ID,Scan_ID,@i:=@i+1 as num from (select ID,Scan_ID from {0} group by ID order by Scan_ID) t2) t1 on t.ID=t1.ID set DatabaseID=num;".format(table),multi=True): pass
        con.commit()

if __name__== "__main__":
    with MrpecDatabaseUpdator() as (d,con,cur):
        
        update = 'standard'
        
        ##update database with deidentified column IDs        
        #d.update_database_ID(update)
                
        d.insert_additional_metabolites(update, d.met_to_calculate)
        d.insert_aggregate_metabolites_optimal(update, d.met_to_calculate)
        
        ##rename Cr and Cho for 1.5T scans to tCr and tCho
        if d.execute_and_return_query("select count(Cr) from standard where ScanBZero=1.5")[1][0][0]:
            print("\nRenamed Cr and Cho from 1.5T to tCr and tCho (and tCr_opt,tCho_opt)")
            cur.execute("UPDATE standard SET tCr=Cr,tCho=Cho,`tCr_%SD`=`Cr_%SD`,`tCho_%SD`=`Cho_%SD` WHERE ScanBZero=1.5 AND standard.Scan_ID=Scan_ID")
            cur.execute("UPDATE standard SET tCr_opt=Cr WHERE (`tCr_%SD` BETWEEN 0.000001 AND {}) AND ScanBZero=1.5 AND standard.Scan_ID=Scan_ID".format(d.met_threshold['tCr_opt']))
            cur.execute("UPDATE standard SET tCho_opt=Cho WHERE (`tCho_%SD` BETWEEN 0.000001 AND {}) AND ScanBZero=1.5 AND standard.Scan_ID=Scan_ID".format(d.met_threshold['tCho_opt']))
            con.commit()
            cur.execute("UPDATE standard SET Cr=NULL,Cho=NULL,`Cr_%SD`=NULL,`Cho_%SD`=NULL WHERE ScanBZero=1.5")
            con.commit()           
        
                
        if d.prompt_yes_no("\nDo you wish to create/overwrite the windowed SD columns with null values? WARNING: This cannot be undone, and will take a long time to restore the values."): d.create_null_sd_columns(update)
        