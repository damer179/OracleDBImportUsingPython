import cx_Oracle
import config as cfg

from datetime import date

from drop import *
from create import *

connection = cx_Oracle.connect(cfg.username,
                            cfg.password,
                            cfg.dsn,
                            cx_Oracle.SYSDBA)

print("Successfully connected to Oracle Database --> " + cfg.dsn)

# Get the config values
cur = connection.cursor()
cur.execute("select * from SYS.DB_IMPORT_CONFIG") 
for ORACLE_HOME, IMPORT_FILE_LOCATION, CURRENT_IMPORT_FILE in cur:
    print("ORACLE_HOME: ", ORACLE_HOME)
    print("Data Pump Directory location: ", IMPORT_FILE_LOCATION)
    print("Current Import File: ", CURRENT_IMPORT_FILE)

### Call Drop Procs
dropProcs(cur)

### Call Create Proc
createProcs(cur)


######################### Actual Import Process #########################################

print("Starting the Actual Import Process....")

today = date.today()
d1 = today.strftime("%m-%d-%Y")

#print(ORACLE_HOME + "/bin/impdp \"/ as sysdba\" directory=" + IMPORT_FILE_LOCATION + " DUMPFILE=" + CURRENT_IMPORT_FILE + " logfile=import_action-" + d1 + ".log EXCLUDE=PASSWORD_HISTORY EXCLUDE=TABLESPACE EXCLUDE=JOB EXCLUDE=TABLE:\"IN ('AUDIT_HISTORY')\"" )
#statement_to_run = ORACLE_HOME + "/bin/impdp \"/ as sysdba\" directory=" + IMPORT_FILE_LOCATION + " DUMPFILE=" + CURRENT_IMPORT_FILE + " logfile=import_action-" + d1 + ".log EXCLUDE=PASSWORD_HISTORY EXCLUDE=TABLESPACE EXCLUDE=JOB EXCLUDE=TABLE:\"IN ('AUDIT_HISTORY')\""

#print (statement_to_run)

## Calling Import DP
#try:
#        print("Calling DB_IMPORT_PROCS.DB_RUN_THIS")
#        cur.callproc('DB_IMPORT_PROCS.DB_RUN_THIS', [statement_to_run])
#except cx_Oracle.Error as error:       
#        print("Error encountered in DB_IMPORT_PROCS.DB_RUN_THIS - " + str(error)) 


# Calling Import DP
try:
        print("Calling DB_IMPORT_PROCS.IMPORT_SCHEMA")
        cur.callproc('DB_IMPORT_PROCS.IMPORT_SCHEMA', ['MPEDBA', IMPORT_FILE_LOCATION, CURRENT_IMPORT_FILE])
except cx_Oracle.Error as error:       
        print("Error encountered in DB_IMPORT_PROCS.IMPORT_SCHEMA - " + str(error))         

print("Process has been completed")


#Re-create Public Synonyms for mpedba objects
# Calling Import DP
try:
        print("Calling DB_IMPORT_PROCS.DB_RECREATE_PUBLIC_SYNONYMS")
        cur.callproc('DB_IMPORT_PROCS.DB_RECREATE_PUBLIC_SYNONYMS')
except cx_Oracle.Error as error:       
        print("Error encountered in DB_IMPORT_PROCS.DB_RECREATE_PUBLIC_SYNONYMS - " + str(error))   

# Update FSA CSA Array RPMs
try:
            print("Calling DB_IMPORT_PROCS.DB_UPDATE_FSA_CSA_RPM_ARRAY")
            cur.callproc('DB_IMPORT_PROCS.DB_UPDATE_FSA_CSA_RPM_ARRAY')
except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_UPDATE_FSA_CSA_RPM_ARRAY - " + str(error)) 

# Update IDF Link Account
try:
            print("Calling DB_IMPORT_PROCS.DB_ALTER_IDF_LINK")
            cur.callproc('DB_IMPORT_PROCS.DB_ALTER_IDF_LINK')
except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_ALTER_IDF_LINK - " + str(error))    
'''
# Create Person Sequence
try:
            print("Calling DB_IMPORT_PROCS.DB_CREATE_PERSON_SEQ")
            cur.callproc('DB_IMPORT_PROCS.DB_CREATE_PERSON_SEQ')
except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_CREATE_PERSON_SEQ - " + str(error)) 



# Create Interface Users
try:
            print("Calling DB_IMPORT_PROCS.DB_CREATE_INTERFACE_USERS")
            cur.callproc('DB_IMPORT_PROCS.DB_CREATE_INTERFACE_USERS')
except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_CREATE_INTERFACE_USERS - " + str(error))                                  

'''

# DB Import has been done - Close connection
connection.close()

print("Successfully disconnected from Oracle Database --> " + cfg.dsn)