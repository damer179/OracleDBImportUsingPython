/* Formatted on 7/9/2021 10:15:01 PM (QP5 v5.365) */
CREATE OR REPLACE PACKAGE BODY SYS.DB_IMPORT_PROCS
AS
    PROCEDURE DB_DROP_MPE_ROLES_PROFILES
    AS
        v_drop_role_statement      VARCHAR2 (500);
        v_drop_profile_statement   VARCHAR2 (500);
    BEGIN
        /* Roles */
        FOR i IN (SELECT DISTINCT role
                    FROM sys.dba_roles
                   WHERE UPPER (ROLE) IN ('UR_REPORT',
                                          'HELPDESK_ROLE',
                                          'SECURITY_ROLE',
                                          'MPE_DBA_ROLE',
                                          'FIX_ID_ROLE',
                                          'OST_USERS_ADMIN',
                                          'OST_ADMIN',
                                          'WEB_ROLE',
                                          'MPE_SCHEMA_OWNER_ROLE',
                                          'IDF_LINK_ROLE'))
        LOOP
            v_drop_role_statement := 'DROP ROLE ' || i.role;

            BEGIN
                EXECUTE IMMEDIATE v_drop_role_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -1919                 -- role does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                            v_drop_role_statement,
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_DROP_MPE_ROLES_PROFILES',
                                            'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_role_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_ROLES_PROFILES');
        END LOOP;

        /* Profiles */
        FOR ii
            IN (SELECT DISTINCT profile
                  FROM dba_profiles
                 WHERE UPPER (PROFILE) IN ('MPE_NONINTER_USER', 'APP_USER'))
        LOOP
            v_drop_profile_statement :=
                'DROP PROFILE ' || ii.profile || ' CASCADE';

            BEGIN
                EXECUTE IMMEDIATE v_drop_profile_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -2380              -- profile does not exists
                    THEN
                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_profile_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_ROLES_PROFILES');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_ROLES_PROFILES;

    PROCEDURE DB_DROP_MPE_SYNONYMS
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT synonym_name
                    FROM dba_synonyms
                   WHERE table_owner = 'MPEDBA')
        LOOP
            v_drop_statement := 'DROP PUBLIC SYNONYM ' || i.synonym_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -1432              -- synonym does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                             VALUES (v_drop_statement,
                                     SYSDATE,
                                     'DB_IMPORT_PROCS.DB_DROP_MPE_SYNONYMS',
                                     'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_SYNONYMS');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_SYNONYMS;

    PROCEDURE DB_DROP_MPE_TRIGGERS
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'TRIGGER')
        LOOP
            v_drop_statement := 'DROP TRIGGER MPEDBA.' || i.object_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -4080              -- trigger does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                             VALUES (v_drop_statement,
                                     SYSDATE,
                                     'DB_IMPORT_PROCS.DB_DROP_MPE_TRIGGERS',
                                     'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_TRIGGERS');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_TRIGGERS;

    PROCEDURE DB_DROP_MPE_FUNCTIONS
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'FUNCTION')
        LOOP
            v_drop_statement := 'DROP FUNCTION MPEDBA. ' || i.object_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -4043               -- object does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                             VALUES (v_drop_statement,
                                     SYSDATE,
                                     'DB_IMPORT_PROCS.DB_DROP_MPE_FUNCTIONS',
                                     'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_FUNCTIONS');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_FUNCTIONS;

    PROCEDURE DB_DROP_MPE_VIEWS
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'VIEW')
        LOOP
            v_drop_statement := 'DROP VIEW MPEDBA. ' || i.object_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -4043                 -- view does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                             VALUES (v_drop_statement,
                                     SYSDATE,
                                     'DB_IMPORT_PROCS.DB_DROP_MPE_VIEWS',
                                     'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_VIEWS');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_VIEWS;


    PROCEDURE DB_DROP_MPE_PKG_BODIES
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'PACKAGE BODY')
        LOOP
            v_drop_statement := 'DROP PACKAGE BODY MPEDBA. ' || i.object_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -4043               -- object does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                            v_drop_statement,
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_DROP_MPE_PKG_BODIES',
                                            'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_PKG_BODIES');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_PKG_BODIES;

    PROCEDURE DB_DROP_MPE_PKG_SPECS
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'PACKAGE')
        LOOP
            v_drop_statement := 'DROP PACKAGE MPEDBA. ' || i.object_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -4043               -- object does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                             VALUES (v_drop_statement,
                                     SYSDATE,
                                     'DB_IMPORT_PROCS.DB_DROP_MPE_PKG_SPECS',
                                     'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_PKG_SPECS');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_PKG_SPECS;

    PROCEDURE DB_DROP_MPE_PROCEDURES
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'PROCEDURE')
        LOOP
            v_drop_statement := 'DROP PROCEDURE MPEDBA. ' || i.object_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -4043               -- object does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                            v_drop_statement,
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_DROP_MPE_PROCEDURES',
                                            'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_PROCEDURES');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_PROCEDURES;

    PROCEDURE DB_DROP_MPE_SEQUENCES
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'SEQUENCE')
        LOOP
            v_drop_statement := 'DROP SEQUENCE MPEDBA. ' || i.object_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -2289             -- sequence does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                             VALUES (v_drop_statement,
                                     SYSDATE,
                                     'DB_IMPORT_PROCS.DB_DROP_MPE_SEQUENCES',
                                     'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_SEQUENCES');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_SEQUENCES;

    PROCEDURE DB_DROP_MPE_TABLES
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT object_name
                    FROM dba_objects
                   WHERE owner = 'MPEDBA' AND object_type = 'TABLE')
        LOOP
            v_drop_statement :=
                   'DROP TABLE MPEDBA. '
                || i.object_name
                || ' CASCADE CONSTRAINTS';

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -942                 -- table does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                             VALUES (v_drop_statement,
                                     SYSDATE,
                                     'DB_IMPORT_PROCS.DB_DROP_MPE_TABLES',
                                     'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_TABLES');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_TABLES;

    PROCEDURE DB_DROP_MPE_BROKEN_JOBS
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT job
                    FROM dba_jobs
                   WHERE UPPER (broken) = 'Y')
        LOOP
            v_drop_statement :=
                'BEGIN DBMS_IJOB.REMOVE (' || i.job || '); END';

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -23421                 -- job does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                               'Job does not exists ['
                                            || v_drop_statement
                                            || ']',
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_DROP_MPE_BROKEN_JOBS',
                                            'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_BROKEN_JOBS');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_BROKEN_JOBS;

    PROCEDURE DB_DROP_MPE_DIRECTORIES
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT directory_name
                    FROM dba_directories
                   WHERE directory_name IN ('FUNDED_REQ_TEMP_DIR',
                                            'CAFDEX_DIR',
                                            'ABCS_DIR',
                                            'TRC3_DIR'))
        LOOP
            v_drop_statement := 'DROP DIRECTORY ' || i.directory_name;

            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -4043               -- object does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                            v_drop_statement,
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_DROP_MPE_DIRECTORIES',
                                            'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_MPE_DIRECTORIES');
        END LOOP;

        COMMIT;
    END DB_DROP_MPE_DIRECTORIES;

    PROCEDURE DB_DROP_ALL_NON_ORACLE_USERS
    AS
        v_drop_statement   VARCHAR2 (500);
    BEGIN
        FOR i
            IN (SELECT username
                  FROM dba_users
                 WHERE     UPPER (USERNAME) NOT IN ('SYS',
                                                    'SYSTEM',
                                                    'OUTLN',
                                                    'ORACLE_OCM',
                                                    'DIP',
                                                    'XDB',
                                                    'CTXSYS',
                                                    'WMSYS',
                                                    'DBSNMP',
                                                    'MDDATA',
                                                    'SYSMAN',
                                                    'ORDSYS',
                                                    'ORDDATA',
                                                    'OLAPSYS',
                                                    'SCOTT',
                                                    'OWBSYS',
                                                    'MDSYS',
                                                    'EXFSYS',
                                                    'FLOWS_FILES',
                                                    'APEX_PUBLIC_USER',
                                                    'APEX_030200',
                                                    'ANONYMOUS',
                                                    'OWBSYS_AUDIT',
                                                    'MGMT_VIEW',
                                                    'APPQOSSYS',
                                                    'XS$NULL',
                                                    'ORDPLUGINS',
                                                    'SI_INFORMTN_SCHEMA',
                                                    'SPATIAL_WFS_ADMIN_USR',
                                                    'SPATIAL_CSW_ADMIN_USR',
                                                    'FR_OPSS',
                                                    'FR_IAU',
                                                    'FR_WLS_RUNTIME',
                                                    'FR_WLS',
                                                    'FR_IAU_VIEWER',
                                                    'FR_IAU_APPEND',
                                                    'FR_STB',
                                                    'SYSBACKUP',
                                                    'SYSRAC',
                                                    'AUDSYS',
                                                    'SYSDG',
                                                    'SYSKM',
                                                    'OJVMSYS',
                                                    'SYS$UMF',
                                                    'APEX_050000',
                                                    'DBSFWUSER',
                                                    'GGSYS',
                                                    'GSMADMIN_INTERNAL',
                                                    'QUESTCODETESTER',
                                                    'OPR$ORACLE',
                                                    'SYS$UMF',
                                                    'APEX_050000',
                                                    'DBSFWUSER',
                                                    'GGSYS',
                                                    'GSMADMIN_INTERNAL',
                                                    'QUESTCODETESTER',
                                                    'OPR$ORACLE')
                       AND UPPER (USERNAME) NOT LIKE '%IAU%'
                       AND UPPER (USERNAME) NOT LIKE '%IAU_APPEND%'
                       AND UPPER (USERNAME) NOT LIKE '%IAU_VIEWER%'
                       AND UPPER (USERNAME) NOT LIKE '%WLS%'
                       AND UPPER (USERNAME) NOT LIKE '%WLS_RUNTIME%'
                       AND UPPER (USERNAME) NOT LIKE '%STB%'
                       AND UPPER (USERNAME) NOT LIKE '%OPSS%')
        LOOP
            v_drop_statement := 'DROP USER ' || i.username || ' CASCADE';


            BEGIN
                EXECUTE IMMEDIATE v_drop_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF (SQLCODE = -1918 OR SQLCODE = -1935 OR SQLCODE = -1722) -- username does not exists or missing user or role name or invalid number
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                               'SQLCode in 1928 or 1935 or 1722:  '
                                            || v_drop_statement,
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_DROP_ALL_NON_ORACLE_USERS',
                                            'Error');

                        CONTINUE;
                    ELSE
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                            v_drop_statement,
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_DROP_ALL_NON_ORACLE_USERS',
                                            'Error');

                        CONTINUE;
                    END IF;
            END;


            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_drop_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_DROP_ALL_NON_ORACLE_USERS');
        END LOOP;

        /* Add these cleanup scripts too - DoD Password Function
        EXECUTE IMMEDIATE 'drop function sys.VERIFY_PASSWORD_DOD_9';

        EXECUTE IMMEDIATE 'drop function sys.VERIFY_PASSWORD_DOD';

        EXECUTE IMMEDIATE 'create or replace PUBLIC synonym VERIFY_PASSWORD_DOD_9 for sys.VERIFY_PASSWORD_DOD_9';

        EXECUTE IMMEDIATE 'grant execute on sys.VERIFY_PASSWORD_DOD_9 to public';

        EXECUTE IMMEDIATE 'grant execute on VERIFY_PASSWORD_DOD_9 to public';*/


        COMMIT;
    END DB_DROP_ALL_NON_ORACLE_USERS;


    PROCEDURE DB_CREATE_MPE_IDX_TABLESPACE
    AS
        v_create_statement   VARCHAR2 (500);
        v_location           VARCHAR2 (500);
        v_exist              INTEGER := 0;
    BEGIN
        SELECT COUNT (*)
          INTO v_exist
          FROM dba_tablespaces
         WHERE UPPER (TABLESPACE_NAME) = 'MPE_IDX_S' AND ROWNUM < 2;

        SELECT SUBSTR (name, 1, INSTR (name, 'system') - 1)
          INTO v_location
          FROM v$datafile
         WHERE file# = 1 AND ROWNUM < 2;

        IF v_exist <= 0
        THEN
            v_create_statement :=
                   'CREATE TABLESPACE MPE_IDX_S BLOCKSIZE 8192 DATAFILE  '''
                || v_location
                || 'mpe_idx_s_01.dbf'' SIZE 1048576000 AUTOEXTEND ON NEXT 524288000 MAXSIZE 1500M EXTENT MANAGEMENT LOCAL UNIFORM SIZE 1048576 ONLINE PERMANENT SEGMENT SPACE MANAGEMENT AUTO';

            BEGIN
                EXECUTE IMMEDIATE v_create_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                  date_time,
                                                  comments,
                                                  req_inputs)
                             VALUES (
                                        v_create_statement,
                                        SYSDATE,
                                        'DB_IMPORT_PROCS.DB_CREATE_MPE_IDX_TABLESPACE',
                                        'Error');
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_create_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_CREATE_MPE_IDX_TABLESPACE');
        ELSE
            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES ('Tablespace MPE_IDX already exists',
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_CREATE_MPE_IDX_TABLESPACE');
        END IF;

        COMMIT;
    END DB_CREATE_MPE_IDX_TABLESPACE;

    PROCEDURE DB_CREATE_MPE_TAB_TABLESPACE
    AS
        v_create_statement   VARCHAR2 (500);
        v_location           VARCHAR2 (500);
        v_exist              INTEGER := 0;
    BEGIN
        SELECT COUNT (*)
          INTO v_exist
          FROM dba_tablespaces
         WHERE UPPER (TABLESPACE_NAME) = 'MPE_TAB_S' AND ROWNUM < 2;

        SELECT SUBSTR (name, 1, INSTR (name, 'system') - 1)
          INTO v_location
          FROM v$datafile
         WHERE file# = 1 AND ROWNUM < 2;

        IF v_exist <= 0
        THEN
            v_create_statement :=
                   'CREATE TABLESPACE MPE_TAB_S BLOCKSIZE 8192 DATAFILE  '''
                || v_location
                || 'mpe_tab_s_01.dbf'' SIZE 1048576000 AUTOEXTEND ON NEXT 524288000 MAXSIZE 1500M EXTENT MANAGEMENT LOCAL UNIFORM SIZE 1048576 ONLINE PERMANENT SEGMENT SPACE MANAGEMENT AUTO';

            BEGIN
                EXECUTE IMMEDIATE v_create_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                  date_time,
                                                  comments,
                                                  req_inputs)
                             VALUES (
                                        v_create_statement,
                                        SYSDATE,
                                        'DB_IMPORT_PROCS.DB_CREATE_MPE_TAB_TABLESPACE',
                                        'Error');
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_create_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_CREATE_MPE_TAB_TABLESPACE');
        ELSE
            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES ('Tablespace MPE_IDX already exists',
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_CREATE_MPE_TAB_TABLESPACE');
        END IF;

        COMMIT;
    END DB_CREATE_MPE_TAB_TABLESPACE;


    PROCEDURE DB_CREATE_DEV_PROFILE
    AS
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE 'DROP PROFILE PROFILE_DEV_ONLY CASCADE';
        EXCEPTION
            WHEN OTHERS
            THEN
                IF (SQLCODE = -2380 OR SQLCODE = -1722) -- profile does not exists
                THEN
                    INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                  date_time,
                                                  comments,
                                                  req_inputs)
                         VALUES ('DROP PROFILE PROFILE_DEV_ONLY CASCADE',
                                 SYSDATE,
                                 'DB_IMPORT_PROCS.DB_CREATE_DEV_PROFILE',
                                 'Error');

                    NULL;
                ELSE
                    RAISE;
                END IF;
        END;

        EXECUTE IMMEDIATE 'CREATE PROFILE PROFILE_DEV_ONLY LIMIT 
        PASSWORD_LOCK_TIME UNLIMITED 
        PASSWORD_GRACE_TIME UNLIMITED 
        PASSWORD_LIFE_TIME UNLIMITED
        FAILED_LOGIN_ATTEMPTS UNLIMITED
        PASSWORD_REUSE_TIME UNLIMITED
        PASSWORD_REUSE_MAX UNLIMITED
        PASSWORD_VERIFY_FUNCTION NULL';


        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES ('DEV Profile has been created',
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_CREATE_DEV_PROFILE');
    END DB_CREATE_DEV_PROFILE;


    PROCEDURE DB_UPDATE_FSA_CSA_RPM_ARRAY
    AS
        v_alter_profile   VARCHAR2 (200);
        v_alter_passwd    VARCHAR2 (200);
    BEGIN
        FOR i IN (SELECT username, profile
                    FROM dba_users
                   WHERE UPPER (username) IN ('REGANGM',
                                              'RDGONZALES',
                                              'CSTRONG',
                                              'CKING',
                                              'YPHILLIPS',
                                              'AABROWN',
                                              'AADAVIS',
                                              'BOBSTCM',
                                              'JPABISALEH',
                                              'SCSERVILLON',
                                              'TEBEST',
                                              'WEBPROXY',
                                              'SYS',
                                              'SYSTEM'))
        LOOP
            v_alter_profile :=
                'ALTER USER ' || i.username || ' PROFILE PROFILE_DEV_ONLY';

            EXECUTE IMMEDIATE v_alter_profile;

            v_alter_passwd :=
                'ALTER USER ' || i.username || ' IDENTIFIED BY welcome1';

            EXECUTE IMMEDIATE v_alter_passwd;
        END LOOP;

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES ('DEV RPM  Users has been updated',
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_UPDATE_FSA_CSA_RPM_ARRAY');

        COMMIT;
    END DB_UPDATE_FSA_CSA_RPM_ARRAY;

    PROCEDURE DB_CREATE_MPEDBA
    AS
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE 'CREATE USER MPEDBA identified by welcome1 PROFILE PROFILE_DEV_ONLY DEFAULT TABLESPACE MPE_TAB_S TEMPORARY TABLESPACE TEMP ACCOUNT UNLOCK';
        EXCEPTION
            WHEN OTHERS
            THEN
                IF SQLCODE = -1920               -- mpedba user already exists
                THEN
                    INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                  date_time,
                                                  comments,
                                                  req_inputs)
                         VALUES ('MPEDBA user already exists',
                                 SYSDATE,
                                 'DB_IMPORT_PROCS.DB_CREATE_MPEDBA',
                                 'Error');
                END IF;
        END;


        EXECUTE IMMEDIATE 'ALTER USER MPEDBA identified by welcome1 PROFILE PROFILE_DEV_ONLY';

        EXECUTE IMMEDIATE 'ALTER USER MPEDBA QUOTA UNLIMITED ON MPE_TAB_S';

        EXECUTE IMMEDIATE ('ALTER USER MPEDBA QUOTA UNLIMITED ON MPE_IDX_S');

        EXECUTE IMMEDIATE ('GRANT ADVISOR TO MPEDBA');

        EXECUTE IMMEDIATE ('GRANT CONNECT TO MPEDBA');

        EXECUTE IMMEDIATE ('GRANT CREATE SESSION TO MPEDBA');

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES ('MPEDBA user created',
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_CREATE_MPEDBA');


        COMMIT;
    END DB_CREATE_MPEDBA;


    PROCEDURE DB_CREATE_MPEADMIN
    AS
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE 'CREATE USER MPEADMIN IDENTIFIED BY welcome1 PROFILE PROFILE_DEV_ONLY DEFAULT TABLESPACE MPE_TAB_S TEMPORARY TABLESPACE TEMP ACCOUNT UNLOCK';
        EXCEPTION
            WHEN OTHERS
            THEN
                IF SQLCODE = -1920               -- mpedba user already exists
                THEN
                    INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                  date_time,
                                                  comments,
                                                  req_inputs)
                         VALUES ('MPEADMIN user already exists',
                                 SYSDATE,
                                 'DB_IMPORT_PROCS.DB_CREATE_MPEADMIN',
                                 'Error');
                END IF;
        END;

        EXECUTE IMMEDIATE 'ALTER USER MPEADMIN IDENTIFIED BY welcome1 PROFILE PROFILE_DEV_ONLY';

        EXECUTE IMMEDIATE 'ALTER USER MPEADMIN QUOTA UNLIMITED ON MPE_TAB_S';

        EXECUTE IMMEDIATE('ALTER USER MPEADMIN QUOTA UNLIMITED ON MPE_IDX_S');

        EXECUTE IMMEDIATE ('GRANT ADVISOR TO MPEADMIN');

        EXECUTE IMMEDIATE ('GRANT connect TO MPEADMIN');

        EXECUTE IMMEDIATE ('GRANT CREATE SESSION TO MPEADMIN');

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES ('MPEDBA user created',
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_CREATE_MPEADMIN');


        COMMIT;
    END DB_CREATE_MPEADMIN;


    FUNCTION VERIFY_PASSWORD_DOD_9 (username       VARCHAR2,
                                    password       VARCHAR2,
                                    old_password   VARCHAR2)
        RETURN BOOLEAN
    IS
        n                BOOLEAN;
        m                INTEGER;
        differ           INTEGER;
        isdigit          BOOLEAN;
        numdigit         INTEGER;
        ispunct          BOOLEAN;
        numpunct         INTEGER;
        islowchar        BOOLEAN;
        numlowchar       INTEGER;
        isupchar         BOOLEAN;
        numupchar        INTEGER;
        digitarray       VARCHAR2 (10);
        punctarray       VARCHAR2 (25);
        lowchararray     VARCHAR2 (26);
        upchararray      VARCHAR2 (26);
        pw_change_time   DATE;
    BEGIN
        digitarray := '0123456789';
        lowchararray := 'abcdefghijklmnopqrstuvwxyz';
        upchararray := 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        punctarray := '@!"#$%&()``*+,-/:;<=>?_';

        -- Check if the password is same as the username
        IF NLS_LOWER (password) = NLS_LOWER (username)
        THEN
            raise_application_error (-20001,
                                     'Password same as or similar to user');
        END IF;

        -- Check for the minimum length of the password
        IF LENGTH (password) < 9
        THEN
            raise_application_error (-20002, 'Password length less than 9');
        END IF;

        -- Check if the password is too simple. A dictionary of words may be maintained
        -- and a check may be made so as not to allow the words that are too simple for
        -- the password.
        IF NLS_LOWER (password) IN ('welcome',
                                    'database',
                                    'account',
                                    'user',
                                    'password',
                                    'oracle',
                                    'computer',
                                    'abcdefgh',
                                    '12345')
        THEN
            raise_application_error (-20002, 'Password too simple');
        END IF;

        -- Check if the password contains at least two each of the following:
        -- uppercase characters, lowercase characters, digits and special characters.
        -- 1. Check for the digits
        isdigit := FALSE;
        numdigit := 0;
        m := LENGTH (password);

        FOR i IN 1 .. 10
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (digitarray, i, 1)
                THEN
                    numdigit := numdigit + 1;
                END IF;

                IF numdigit > 1
                THEN
                    isdigit := TRUE;
                    GOTO findlowchar;
                END IF;
            END LOOP;
        END LOOP;

        IF isdigit = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two digits');
        END IF;

       -- 2. Check for the lowercase characters
       <<findlowchar>>
        islowchar := FALSE;
        numlowchar := 0;
        m := LENGTH (password);

        FOR i IN 1 .. LENGTH (lowchararray)
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (lowchararray, i, 1)
                THEN
                    numlowchar := numlowchar + 1;
                END IF;

                IF numlowchar > 1
                THEN
                    islowchar := TRUE;
                    GOTO findupchar;
                END IF;
            END LOOP;
        END LOOP;

        IF islowchar = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two lowercase characters');
        END IF;

       -- 3. Check for the UPPERCASE characters
       <<findupchar>>
        isupchar := FALSE;
        numupchar := 0;
        m := LENGTH (password);

        FOR i IN 1 .. LENGTH (upchararray)
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (upchararray, i, 1)
                THEN
                    numupchar := numupchar + 1;
                END IF;

                IF numupchar > 1
                THEN
                    isupchar := TRUE;
                    GOTO findpunct;
                END IF;
            END LOOP;
        END LOOP;

        IF isupchar = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two uppercase characters');
        END IF;

       -- 4. Check for the punctuation
       <<findpunct>>
        ispunct := FALSE;
        numpunct := 0;
        m := LENGTH (password);

        FOR i IN 1 .. LENGTH (punctarray)
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (punctarray, i, 1)
                THEN
                    numpunct := numpunct + 1;
                END IF;

                IF numpunct > 1
                THEN
                    ispunct := TRUE;
                    GOTO endsearch;
                END IF;
            END LOOP;
        END LOOP;

        IF ispunct = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two punctuation characters');
        END IF;

       -- Check if the password differs from the previous password
       -- by more than 4 characters
       <<endsearch>>
        IF old_password IS NOT NULL
        THEN
            differ := LENGTH (old_password) - LENGTH (password);

            IF ABS (differ) < 4
            THEN
                IF LENGTH (password) < LENGTH (old_password)
                THEN
                    m := LENGTH (password);
                ELSE
                    m := LENGTH (old_password);
                END IF;

                differ := ABS (differ);

                FOR i IN 1 .. m
                LOOP
                    IF SUBSTR (password, i, 1) != SUBSTR (old_password, i, 1)
                    THEN
                        differ := differ + 1;
                    END IF;
                END LOOP;

                IF differ < 4
                THEN
                    raise_application_error (
                        -20004,
                        'Password should differ by more than 4 characters');
                END IF;
            END IF;
        END IF;

        -- Everything is fine. return TRUE
        RETURN (TRUE);
    EXCEPTION
        WHEN OTHERS
        THEN
            raise_application_error (
                -20000,
                'verify_password_dod: Unexpected error: ' || SQLERRM,
                TRUE);
    END VERIFY_PASSWORD_DOD_9;

    FUNCTION VERIFY_PASSWORD_DOD (username       VARCHAR2,
                                  password       VARCHAR2,
                                  old_password   VARCHAR2)
        RETURN BOOLEAN
    IS
        n                BOOLEAN;
        m                INTEGER;
        differ           INTEGER;
        isdigit          BOOLEAN;
        numdigit         INTEGER;
        ispunct          BOOLEAN;
        numpunct         INTEGER;
        islowchar        BOOLEAN;
        numlowchar       INTEGER;
        isupchar         BOOLEAN;
        numupchar        INTEGER;
        digitarray       VARCHAR2 (10);
        punctarray       VARCHAR2 (25);
        lowchararray     VARCHAR2 (26);
        upchararray      VARCHAR2 (26);
        pw_change_time   DATE;
    BEGIN
        digitarray := '0123456789';
        lowchararray := 'abcdefghijklmnopqrstuvwxyz';
        upchararray := 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        punctarray := '@!"#$%&()``*+,-/:; <=>?_';

        -- Check if the password is same as the username
        IF NLS_LOWER (password) = NLS_LOWER (username)
        THEN
            raise_application_error (-20001,
                                     'Password same as or similar to user');
        END IF;

        -- Check for the minimum length of the password
        IF LENGTH (password) < 15
        THEN
            raise_application_error (-20002, 'Password length less than 15');
        END IF;

        -- Check if the password is too simple. A dictionary of words may be maintained
        -- and a check may be made so as not to allow the words that are too simple for
        -- the password.
        IF NLS_LOWER (password) IN ('welcome',
                                    'database',
                                    'account',
                                    'user',
                                    'password',
                                    'oracle',
                                    'computer',
                                    'abcdefgh',
                                    '12345')
        THEN
            raise_application_error (-20002, 'Password too simple');
        END IF;

        -- Check if the password contains at least two each of the following:
        -- uppercase characters, lowercase characters, digits and special characters.
        -- 1. Check for the digits
        isdigit := FALSE;
        numdigit := 0;
        m := LENGTH (password);

        FOR i IN 1 .. 10
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (digitarray, i, 1)
                THEN
                    numdigit := numdigit + 1;
                END IF;

                IF numdigit > 1
                THEN
                    isdigit := TRUE;
                    GOTO findlowchar;
                END IF;
            END LOOP;
        END LOOP;

        IF isdigit = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two digits');
        END IF;

       -- 2. Check for the lowercase characters
       <<findlowchar>>
        islowchar := FALSE;
        numlowchar := 0;
        m := LENGTH (password);

        FOR i IN 1 .. LENGTH (lowchararray)
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (lowchararray, i, 1)
                THEN
                    numlowchar := numlowchar + 1;
                END IF;

                IF numlowchar > 1
                THEN
                    islowchar := TRUE;
                    GOTO findupchar;
                END IF;
            END LOOP;
        END LOOP;

        IF islowchar = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two lowercase characters');
        END IF;

       -- 3. Check for the UPPERCASE characters
       <<findupchar>>
        isupchar := FALSE;
        numupchar := 0;
        m := LENGTH (password);

        FOR i IN 1 .. LENGTH (upchararray)
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (upchararray, i, 1)
                THEN
                    numupchar := numupchar + 1;
                END IF;

                IF numupchar > 1
                THEN
                    isupchar := TRUE;
                    GOTO findpunct;
                END IF;
            END LOOP;
        END LOOP;

        IF isupchar = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two uppercase characters');
        END IF;

       -- 4. Check for the punctuation
       <<findpunct>>
        ispunct := FALSE;
        numpunct := 0;
        m := LENGTH (password);

        FOR i IN 1 .. LENGTH (punctarray)
        LOOP
            FOR j IN 1 .. m
            LOOP
                IF SUBSTR (password, j, 1) = SUBSTR (punctarray, i, 1)
                THEN
                    numpunct := numpunct + 1;
                END IF;

                IF numpunct > 1
                THEN
                    ispunct := TRUE;
                    GOTO endsearch;
                END IF;
            END LOOP;
        END LOOP;

        IF ispunct = FALSE
        THEN
            raise_application_error (
                -20003,
                'Password should contain at least two punctuation characters');
        END IF;

       -- Check if the password differs from the previous password
       -- by more than 4 characters
       <<endsearch>>
        IF old_password IS NOT NULL
        THEN
            differ := LENGTH (old_password) - LENGTH (password);

            IF ABS (differ) < 4
            THEN
                IF LENGTH (password) < LENGTH (old_password)
                THEN
                    m := LENGTH (password);
                ELSE
                    m := LENGTH (old_password);
                END IF;

                differ := ABS (differ);

                FOR i IN 1 .. m
                LOOP
                    IF SUBSTR (password, i, 1) != SUBSTR (old_password, i, 1)
                    THEN
                        differ := differ + 1;
                    END IF;
                END LOOP;

                IF differ < 4
                THEN
                    raise_application_error (
                        -20004,
                        'Password should differ by more than 4 characters');
                END IF;
            END IF;
        END IF;

        -- Everything is fine. return TRUE
        RETURN (TRUE);
    EXCEPTION
        WHEN OTHERS
        THEN
            raise_application_error (
                -20000,
                'verify_password_dod: Unexpected error: ' || SQLERRM,
                TRUE);
    END VERIFY_PASSWORD_DOD;

    PROCEDURE DB_CREATE_DOD_9_PASSWORD_FUNC
    AS
    BEGIN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE PUBLIC SYNONYM VERIFY_PASSWORD_DOD_9 for sys.VERIFY_PASSWORD_DOD_9';

        EXECUTE IMMEDIATE 'GRANT EXECUTE ON SYS.VERIFY_PASSWORD_DOD_9 to public';

        EXECUTE IMMEDIATE 'GRANT EXECUTE ON SYS.VERIFY_PASSWORD_DOD_9 to public';

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES (
                            'CREATE OR REPLACE PUBLIC SYNONYM VERIFY_PASSWORD_DOD_9 FOR SYS.VERIFY_PASSWORD_DOD_9',
                            SYSDATE,
                            'DB_IMPORT_PROCS.DB_CREATE_DOD_9_PASSWORD_FUNC');
    END DB_CREATE_DOD_9_PASSWORD_FUNC;
    
     PROCEDURE DB_CREATE_DOD_PASSWORD_FUNC
    AS
    BEGIN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE PUBLIC SYNONYM VERIFY_PASSWORD_DOD for SYS.VERIFY_PASSWORD_DOD';

        EXECUTE IMMEDIATE 'GRANT EXECUTE ON SYS.VERIFY_PASSWORD_DOD TO PUBLIC';

        EXECUTE IMMEDIATE 'GRANT EXECUTE ON SYS.VERIFY_PASSWORD_DOD TO PUBLIC';

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES (
                            'CREATE OR REPLACE PUBLIC SYNONYM VERIFY_PASSWORD_DOD FOR SYS.VERIFY_PASSWORD_DOD',
                            SYSDATE,
                            'DB_IMPORT_PROCS.DB_CREATE_DOD_PASSWORD_FUNC');
    END DB_CREATE_DOD_PASSWORD_FUNC;

    PROCEDURE IMPORT_SCHEMA (p_schema                 IN VARCHAR2,
                             p_IMPORT_FILE_LOCATION      VARCHAR2,
                             p_IMPORT_FILE               VARCHAR2)
    AS
        h_pump           NUMBER;
        v_current_time   DATE := SYSDATE; -- consistent timestamp for files, job_name etc.
        v_start_time     DATE;                      -- start time for log file
        v_job_name       VARCHAR2 (4000);                  -- job name created
        v_schema_name    VARCHAR2 (30);                 -- schema name created
        v_default_dir    VARCHAR (30) := 'DBBACKUP';              -- directory
        v_logfile_name   VARCHAR2 (200);                       -- logfile name
        v_degree         INTEGER := 2;                -- degree of parallelism
        v_line_no        INTEGER := 0;                        -- debug line no
        v_code           NUMBER;                             -- error sql code
        v_errm           VARCHAR2 (64);                       -- error message

        job_state        VARCHAR2 (30);          -- To keep track of job state
        percent_done     NUMBER;                 -- Percentage of job complete
        le               ku$_LogEntry;           -- For WIP and error messages
        js               ku$_JobStatus;      -- The job status from get_status
        jd               ku$_JobDesc;   -- The job description from get_status
        sts              ku$_Status; -- The status object returned by get_status
        ind              NUMBER;                                 -- Loop index
        vnum             INTEGER := 8;
    BEGIN
        BEGIN
            v_schema_name := UPPER (p_schema);
            v_job_name := 'REFRESH_SCHEMA_' || v_schema_name;

            v_logfile_name :=
                   'refresh_schema_'
                || v_schema_name
                || '_'
                || TO_CHAR (v_current_time, 'YYYYMMDD_HH24MI')
                || '.log';

            h_pump :=
                DBMS_DATAPUMP.open (operation     => 'IMPORT',
                                    job_mode      => 'SCHEMA',
                                    remote_link   => NULL,           --'PROD',
                                    job_name      => v_job_name,
                                    version       => 'LATEST');

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('After DBMS_DATAPUMP.open ...',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         1);

            -- Specify the dump file name and directory object name
            DBMS_DATAPUMP.add_file (handle      => h_pump,
                                    filename    => p_IMPORT_FILE,
                                    directory   => 'DMPDIR');

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('After DBMS_DATAPUMP.add_file ...',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         2);

            -- Specify the log file name and directory object name.
            DBMS_DATAPUMP.add_file (
                handle      => h_pump,
                filename    => v_logfile_name,
                directory   => 'DMPDIR',
                filetype    => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('After DBMS_DATAPUMP.add_file ...',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         3);

            -- filter for schema
            DBMS_DATAPUMP.metadata_filter (
                handle   => h_pump,
                name     => 'SCHEMA_LIST',
                VALUE    => '''' || v_schema_name || '''');

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('After DBMS_DATAPUMP.metadata_filter ...',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         4);

            -- add option
            DBMS_DATAPUMP.set_parameter (handle   => h_pump,
                                         name     => 'TABLE_EXISTS_ACTION',
                                         VALUE    => 'REPLACE');

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('After DBMS_DATAPUMP.set_parameter ...',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         5);

            --
            -- add parallelism
            DBMS_DATAPUMP.set_parallel (handle => h_pump, degree => v_degree);

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('After DBMS_DATAPUMP.set_parallel ...',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         6);

            -- start datapump
            DBMS_DATAPUMP.start_job (handle => h_pump);

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('After DBMS_DATAPUMP.start_job ...',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         7);


            -- The export job should now be running. In the following loop, the job
            -- is monitored until it completes. In the meantime, progress information is
            -- displayed.

            percent_done := 0;
            job_state := 'UNDEFINED';

            WHILE (job_state != 'COMPLETED') AND (job_state != 'STOPPED')
            LOOP
                DBMS_DATAPUMP.get_status (
                    h_pump,
                      DBMS_DATAPUMP.ku$_status_job_error
                    + DBMS_DATAPUMP.ku$_status_job_status
                    + DBMS_DATAPUMP.ku$_status_wip,
                    -1,
                    job_state,
                    sts);
                js := sts.job_status;


                -- If the percentage done changed, display the new value.
                IF js.percent_done != percent_done
                THEN
                    vnum := vnum + 1;

                    INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                  date_time,
                                                  comments,
                                                  seq)
                             VALUES (
                                           '*** Job percent done = ['
                                        || TO_CHAR (js.percent_done)
                                        || '%]',
                                        SYSDATE,
                                        'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                                        vnum);

                    DBMS_OUTPUT.put_line (
                           '*** Job percent done = '
                        || TO_CHAR (js.percent_done));

                    percent_done := js.percent_done;
                END IF;

                -- If any work-in-progress (WIP) or error messages were received for the job,
                -- display them.

                IF (BITAND (sts.mask, DBMS_DATAPUMP.ku$_status_wip) != 0)
                THEN
                    le := sts.wip;
                ELSE
                    IF (BITAND (sts.mask, DBMS_DATAPUMP.ku$_status_job_error) !=
                        0)
                    THEN
                        le := sts.error;
                    ELSE
                        le := NULL;
                    END IF;
                END IF;

                IF le IS NOT NULL
                THEN
                    ind := le.FIRST;

                    WHILE ind IS NOT NULL
                    LOOP
                        DBMS_OUTPUT.put_line (le (ind).LogText);
                        ind := le.NEXT (ind);
                    END LOOP;
                END IF;
            END LOOP;

            -- Indicate that the job finished and detach from it.
            DBMS_OUTPUT.put_line ('Job has completed');
            DBMS_OUTPUT.put_line ('Final job state = ' || job_state);

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('Job has completed',
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         vnum);

            INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                          date_time,
                                          comments,
                                          seq)
                 VALUES ('Final job state = ' || job_state,
                         SYSDATE,
                         'DB_IMPORT_PROCS.IMPORT_SCHEMA',
                         vnum);

            -- end of datapump
            DBMS_DATAPUMP.detach (handle => h_pump);

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES ('Schema has been imported',
                             SYSDATE,
                             'DB_IMPORT_PROCS.IMPORT_SCHEMA');
        EXCEPTION
            WHEN OTHERS
            THEN
                --dbms_output.put_line('Error:' || sqlerrm || ' on Job-ID:' || h_pump );
                v_code := SQLCODE;
                v_errm := SUBSTR (SQLERRM, 1, 64);

                INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                              date_time,
                                              comments)
                         VALUES (
                                       'MESSAGE:  Schema ['
                                    || p_schema
                                    || '] on Job-ID: ['
                                    || h_pump
                                    || ']',
                                    SYSDATE,
                                    'DB_IMPORT_PROCS.IMPORT_SCHEMA');

                INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                              date_time,
                                              comments)
                         VALUES (
                                       'ERROR CODE: ['
                                    || v_code
                                    || '] - '
                                    || v_errm,
                                    SYSDATE,
                                    'DB_IMPORT_PROCS.IMPORT_SCHEMA');
        END;

        COMMIT;
    END IMPORT_SCHEMA;

    PROCEDURE DB_RUN_THIS (statement_to_run VARCHAR2)
    AS
        updated_statement_to_run   VARCHAR2 (5000);
    BEGIN
        updated_statement_to_run := '''' || statement_to_run || '''';

        EXECUTE IMMEDIATE '/ora03/app/oracle/product/12.2.0/dbhome_1/bin/impdp "/ as sysdba" directory=/ora03/app/oracle/admin/MPEPROD/dpdump DUMPFILE=mpeprod_20210404_030002_01.dmp logfile=import_action-07-02-2021.log EXCLUDE=PASSWORD_HISTORY EXCLUDE=TABLESPACE EXCLUDE=JOB ';

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES (updated_statement_to_run,
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_RUN_THIS');


        COMMIT;
    END DB_RUN_THIS;


    PROCEDURE DB_RECREATE_PUBLIC_SYNONYMS
    AS
        v_create_statement   VARCHAR2 (500);
    BEGIN
        FOR i IN (SELECT synonym_name
                    FROM dba_synonyms
                   WHERE table_owner = 'MPEDBA')
        LOOP
            v_create_statement :=
                'CREATE OR REPLACE PUBLIC SYNONYM ' || i.synonym_name;

            BEGIN
                EXECUTE IMMEDIATE v_create_statement;
            EXCEPTION
                WHEN OTHERS
                THEN
                    IF SQLCODE = -1432              -- synonym does not exists
                    THEN
                        INSERT INTO SYS.IMPORT_DEBUG (sql_statement,
                                                      date_time,
                                                      comments,
                                                      req_inputs)
                                 VALUES (
                                            v_create_statement,
                                            SYSDATE,
                                            'DB_IMPORT_PROCS.DB_RECREATE_PUBLIC_SYNONYMS',
                                            'Error');

                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;

            /* Debug */
            INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                     VALUES (v_create_statement,
                             SYSDATE,
                             'DB_IMPORT_PROCS.DB_RECREATE_PUBLIC_SYNONYMS');
        END LOOP;

        COMMIT;
    END DB_RECREATE_PUBLIC_SYNONYMS;

    PROCEDURE DB_ALTER_IDF_LINK
    AS
        v_alter_statement   VARCHAR2 (200);
    BEGIN
        FOR i IN (SELECT username, profile
                    FROM dba_users
                   WHERE UPPER (username) IN ('IDF_LINK'))
        LOOP
            v_alter_statement :=
                'ALTER ' || i.username || ' PROFILE PROFILE_DEV_ONLY';

            EXECUTE IMMEDIATE v_alter_statement;

            v_alter_statement :=
                'ALTER ' || i.username || ' IDENTIFIED BY welcome1';

            EXECUTE IMMEDIATE v_alter_statement;
        END LOOP;

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES (v_alter_statement,
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_ALTER_IDF_LINK');


        COMMIT;
    END DB_ALTER_IDF_LINK;

    PROCEDURE DB_CREATE_INTERFACE_USERS
    AS
        v_create_statement   VARCHAR2 (200);
        v_alter_statement    VARCHAR2 (200);
        v_username           dba_users.username%TYPE;
        v_out_error_code     NUMBER;
        v_out_person_id      VARCHAR2 (30);

        TYPE array_t IS TABLE OF dba_users.username%TYPE;

        user_list            array_t
                                 := array_t ('devhoth',
                                             'devecho',
                                             'testuser',
                                             'uetuser',
                                             'produser');
        orgs_list            array_t := array_t ('OCAC', 'OGDN', 'WRAL');
        v_org_cd             VARCHAR2 (4) := 'OCAC';
    BEGIN
        --        DB_CREATE_PERSON_SEQ;


        FOR i IN 1 .. user_list.COUNT
        LOOP
            v_username := user_list (i);

            v_create_statement :=
                   'CREATE USER '
                || v_username
                || ' IDENTIFIED BY welcome1 PROFILE PROFILE_DEV_ONLY DEFAULT TABLESPACE MPE_TAB_S TEMPORARY TABLESPACE TEMP ACCOUNT UNLOCK';

            EXECUTE IMMEDIATE v_create_statement;

            v_alter_statement :=
                   'ALTER USER '
                || v_username
                || ' QUOTA UNLIMITED ON MPE_TAB_S';

            EXECUTE IMMEDIATE v_alter_statement;

            v_alter_statement :=
                   'ALTER USER '
                || v_username
                || ' QUOTA UNLIMITED ON MPE_IDX_S';

            EXECUTE IMMEDIATE v_alter_statement;

            -- Grants
            EXECUTE IMMEDIATE ('GRANT ur_report TO ' || v_username);

            EXECUTE IMMEDIATE ('GRANT web_role TO ' || v_username);

            EXECUTE IMMEDIATE ('GRANT appadmin TO ' || v_username);

            EXECUTE IMMEDIATE ('GRANT dba TO ' || v_username);

            EXECUTE IMMEDIATE(   'GRANT EXECUTE ON MPEDBA.PK_READ_BATCH_EXEC_BY_USER to '
                              || v_username);

            EXECUTE IMMEDIATE(   'GRANT EXECUTE ON MPEDBA.PK_READ_PSEUDO_DATA to '
                              || v_username);

            EXECUTE IMMEDIATE(   'GRANT SELECT ON MPEDBA.MPE_LOCAL_DECODE to '
                              || v_username);

            EXECUTE IMMEDIATE(   'GRANT SELECT ON MPEDBA.MPE_SHARED_DECODE to '
                              || v_username);

            EXECUTE IMMEDIATE(   'GRANT SELECT on MPEDBA.MPE_DECODE to '
                              || v_username);

            EXECUTE IMMEDIATE(   'ALTER USER '
                              || v_username
                              || ' GRANT CONNECT THROUGH WEBPROXY');

            -- Process per ORG
            FOR i IN 1 .. orgs_list.COUNT
            LOOP
                v_org_cd := orgs_list (i);
            --                -- Code copied from MPEDBA.SP_create_persons
            --                SELECT PERSON_ID_SEQ.NEXTVAL INTO v_out_person_id FROM DUAL;
            --
            --                INSERT INTO MPE_PERSON (PERSON_ID,
            --                                        FIRST_NAME,
            --                                        MIDDLE_INITIAL_TX,
            --                                        LAST_NAME,
            --                                        ZIP_CD,
            --                                        STATE_CD,
            --                                        CITY_NM,
            --                                        DSN_FAX_PHONE_TX,
            --                                        OFFICE_SYMBOL_TX,
            --                                        DSN_PHONE_TX,
            --                                        ADDRESS_LINE01_TX,
            --                                        ADDRESS_LINE02_TX,
            --                                        COMMAND_POST_CD,
            --                                        SERVICE_CD,
            --                                        FAX_PHONE_TX,
            --                                        PHONE_TX,
            --                                        COUNTRY_CD,
            --                                        MGR_DIVISION_ID,
            --                                        RPM_TYPE_CD,
            --                                        ORGANIZATION_CD)
            --                     VALUES (v_out_person_id,
            --                             v_username,
            --                             NULL,
            --                             v_username,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             NULL,
            --                             'CNTR',
            --                             v_org_cd);


            --                INSERT INTO mpedba.mpe_user (person_id,
            --                                             user_id,
            --                                             up_usrid,
            --                                             up_dt_tm,
            --                                             user_pswd_change_date,
            --                                             organization_cd)
            --                     VALUES (v_out_person_id,
            --                             v_username,
            --                             'SYS',
            --                             SYSDATE,
            --                             SYSDATE + 365,
            --                             v_org_cd);
            --
            --                /*REQQTY*/
            --                INSERT INTO mpedba.mpe_user_role (user_id,
            --                                                  role_id,
            --                                                  up_usrid,
            --                                                  up_dt_tm,
            --                                                  organization_cd)
            --                     VALUES (v_username,
            --                             'REQQTY',
            --                             'SYS',
            --                             SYSDATE,
            --                             v_org_cd);
            --
            --                /*ADDMAN*/
            --                INSERT INTO mpedba.mpe_user_role (user_id,
            --                                                  role_id,
            --                                                  up_usrid,
            --                                                  up_dt_tm,
            --                                                  organization_cd)
            --                     VALUES (v_username,
            --                             'ADDMAN',
            --                             'SYS',
            --                             SYSDATE,
            --                             v_org_cd);
            --
            --
            --                /*MPEUSER*/
            --                INSERT INTO mpedba.mpe_user_role (user_id,
            --                                                  role_id,
            --                                                  up_usrid,
            --                                                  up_dt_tm,
            --                                                  organization_cd)
            --                     VALUES (v_username,
            --                             'MPEUSER',
            --                             'SYS',
            --                             SYSDATE,
            --                             v_org_cd);
            --
            --                /*TRCMOD*/
            --                INSERT INTO mpedba.mpe_user_role (user_id,
            --                                                  role_id,
            --                                                  up_usrid,
            --                                                  up_dt_tm,
            --                                                  organization_cd)
            --                     VALUES (v_username,
            --                             'TRCMOD',
            --                             'SYS',
            --                             SYSDATE,
            --                             v_org_cd);
            --
            --                /*FSA*/
            --                INSERT INTO mpedba.mpe_user_role (user_id,
            --                                                  role_id,
            --                                                  up_usrid,
            --                                                  up_dt_tm,
            --                                                  organization_cd)
            --                     VALUES (v_username,
            --                             'FSA',
            --                             'SYS',
            --                             SYSDATE,
            --                             v_org_cd);
            --
            --                /*r11*/
            --                INSERT INTO mpedba.mpe_user_role (user_id,
            --                                                  role_id,
            --                                                  up_usrid,
            --                                                  up_dt_tm,
            --                                                  organization_cd)
            --                     VALUES (v_username,
            --                             'r11',
            --                             'SYS',
            --                             SYSDATE,
            --                             v_org_cd);
            END LOOP;
        END LOOP;

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES (v_create_statement,
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_CREATE_INTERFACE_USERS');


        COMMIT;
    END DB_CREATE_INTERFACE_USERS;


    PROCEDURE DB_CREATE_PERSON_SEQ
    AS
    BEGIN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE  "MPEDBA"."PERSON_ID_SEQ"  MINVALUE 1 MAXVALUE 9999999999999999 INCREMENT BY 1 START WITH 2482 NOCACHE  NOORDER  CYCLE  NOKEEP  GLOBAL';

        EXECUTE IMMEDIATE 'GRANT SELECT ON "MPEDBA"."PERSON_ID_SEQ" TO "WEB_ROLE"';

        /* Debug */
        INSERT INTO SYS.IMPORT_DEBUG (sql_statement, date_time, comments)
                 VALUES ('PERSON SEQ created',
                         SYSDATE,
                         'DB_IMPORT_PROCS.DB_CREATE_PERSON_SEQ');


        COMMIT;
    END DB_CREATE_PERSON_SEQ;
END DB_IMPORT_PROCS;
/