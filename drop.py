import cx_Oracle

def dropProcs(cur):

    ######################### Drop Existing Objects #######################

    # Drop MPE Package Bodies
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_PKG_BODIES")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_PKG_BODIES')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_PKG_BODIES - " + str(error))

    # Drop MPE Package Specs
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_PKG_SPECS")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_PKG_SPECS')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_PKG_SPECS - " + str(error))

    # Drop MPE Procedures
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_PROCEDURES")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_PROCEDURES')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_PROCEDURES - " + str(error))

    # Drop MPE Sequences
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_SEQUENCES")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_SEQUENCES')
    except cx_Oracle.Error as error:        
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_SEQUENCES - " + str(error))

    # Drop MPE Triggers
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_TRIGGERS")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_TRIGGERS')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_TRIGGERS - " + str(error))    

    # Drop MPE Tables
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_TABLES")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_TABLES')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_TABLES - " + str(error))

    # Drop MPE Roles and Profiles
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_ROLES_PROFILES")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_ROLES_PROFILES')
    except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_ROLES_PROFILES - " + str(error))

    # Drop MPE Synonyms
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_SYNONYMS")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_SYNONYMS')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_SYNONYMS - " + str(error))        

    # Drop MPE Functions
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_FUNCTIONS")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_FUNCTIONS')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_FUNCTIONS - " + str(error))

    # Drop MPE Views
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_VIEWS")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_VIEWS')
    except cx_Oracle.Error as error:        
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_VIEWS - " + str(error))

    # Drop MPE Directories
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_DIRECTORIES")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_DIRECTORIES')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_DIRECTORIES - " + str(error))


    # Drop All Non-Oracle Users
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_ALL_NON_ORACLE_USERS")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_ALL_NON_ORACLE_USERS')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_ALL_NON_ORACLE_USERS - " + str(error))


    # Drop MPE Broken Jobs
    try:
            print("Calling DB_IMPORT_PROCS.DB_DROP_MPE_BROKEN_JOBS")
            cur.callproc('DB_IMPORT_PROCS.DB_DROP_MPE_BROKEN_JOBS')
    except cx_Oracle.Error as error:
            print("Error encountered in DB_IMPORT_PROCS.DB_DROP_MPE_BROKEN_JOBS - " + str(error))        


