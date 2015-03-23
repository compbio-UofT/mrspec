from connection import *
from create_database import *
import sys

metabolites = {'CrCH2':40, 'AcAc':40, 'Acn':40, 'Ala':40, 'Asp':40, 'Cho':20, 'Cr':30,
                 'GABA':40, 'GPC':40, 'Glc':40, 'Gln':40, 'Glu':30, 'Gua':40, 'Ins':30, 'Lac':40, 'Lip09':40,
                 'Lip13a':40, 'Lip13b':40, 'Lip20':40, 'MM09':40, 'MM12':40, 'MM14':40, 'MM17':40, 'MM20':40,
                 'NAA':20, 'NAAG':40, 'PCh':30, 'PCr':40, 'Scyllo':40, 'Tau':40, 'tCr':30, 'tNAA':20, 'tCho':20, 'Glx':30,'tCr_opt':30, 'tNAA_opt':20, 'tCho_opt':20, 'Glx_opt':30}

def update_database_ID(table):
    if not column_exists(table, 'DatabaseID'):
        cur.execute("alter table {} add column DatabaseID BIGINT(21)".format(table))
    for result in cur.execute("set @i=0;update {0} as t inner join (select ID,Scan_ID,@i:=@i+1 as num from (select ID,Scan_ID from {0} group by ID order by Scan_ID) t2) t1 on t.ID=t1.ID set DatabaseID=num;".format(table),multi=True): pass
    con.commit()
    
def create_null_sd_columns(table):
    for m in metabolites:
        if column_exists(table, m+'_SD'):
            cur.execute('UPDATE {} SET {} = NULL'.format(table, m+"_SD"))
        else:
            cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, m+'_SD', 'DECIMAL(11,6)'))
    con.commit()

def query_yes_no(question, default="no"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

if __name__== "__main__":
    with DatabaseConnection(sys.argv) as (con,cur):
        
        update = 'standard'
        
        update_database_ID(update)
        
              
        insert_additional_metabolites(update, met_to_calculate)
        insert_aggregate_metabolites_optimal(update, met_to_calculate)
                
        if query_yes_no("\nDo you wish to create/overwrite the windowed SD columns with null values? WARNING: This cannot be undone, and will take a long time to restore the values."): create_null_sd_columns(update)
        
        #update database with deidentified column IDs
