import cx_Oracle

def createProcs(cur):

    ############ Create Dropped Objects #########################

    # Create MPE IDX Tablespace
    try:
            print("Calling DB_IMPORT_PROCS.DB_CREATE_MPE_IDX_TABLESPACE")
            cur.callproc('DB_IMPORT_PROCS.DB_CREATE_MPE_IDX_TABLESPACE')
    except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_CREATE_MPE_IDX_TABLESPACE - " + str(error))        

    # Create MPE TAB Tablespace
    try:
            print("Calling DB_IMPORT_PROCS.DB_CREATE_MPE_TAB_TABLESPACE")
            cur.callproc('DB_IMPORT_PROCS.DB_CREATE_MPE_TAB_TABLESPACE')
    except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_CREATE_MPE_TAB_TABLESPACE - " + str(error)) 


    ## Create Profiles and Roles
    # Need to create the missing roles and profiles before calling import (?)
    # Snapped back ldevdb01 - 7/9/21 10:26 PM
    #profile MPE_NONINTER_USER 
    #role 'UR_REPORT'


    # Create DEV Profiles
    try:
            print("Calling DB_IMPORT_PROCS.DB_CREATE_DEV_PROFILE")
            cur.callproc('DB_IMPORT_PROCS.DB_CREATE_DEV_PROFILE')
    except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_CREATE_DEV_PROFILE - " + str(error))   

    # Create MPEDBA User
    try:
            print("Calling DB_IMPORT_PROCS.DB_CREATE_MPEDBA")
            cur.callproc('DB_IMPORT_PROCS.DB_CREATE_MPEDBA')
    except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_CREATE_MPEDBA - " + str(error)) 

    # Create MPEADMIN User
    try:
            print("Calling DB_IMPORT_PROCS.DB_CREATE_MPEADMIN")
            cur.callproc('DB_IMPORT_PROCS.DB_CREATE_MPEADMIN')
    except cx_Oracle.Error as error:       
            print("Error encountered in DB_IMPORT_PROCS.DB_CREATE_MPEADMIN - " + str(error))         


   