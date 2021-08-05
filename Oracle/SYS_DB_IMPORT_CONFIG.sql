/* Formatted on 7/9/2021 4:18:42 PM (QP5 v5.365) */
DROP TABLE SYS.DB_IMPORT_CONFIG CASCADE CONSTRAINTS;

CREATE TABLE SYS.DB_IMPORT_CONFIG
(
    ORACLE_HOME             VARCHAR2 (500 BYTE),
    IMPORT_FILE_LOCATION    VARCHAR2 (500 BYTE),
    CURRENT_IMPORT_FILE     VARCHAR2 (500)
);


INSERT INTO SYS.DB_IMPORT_CONFIG (ORACLE_HOME,
                                  IMPORT_FILE_LOCATION,
                                  CURRENT_IMPORT_FILE)
         VALUES ('/ora03/app/oracle/product/12.2.0/dbhome_1',
                 '/ora03/app/oracle/admin/MPEPROD/dpdump',
                 'mpeprod_20210404_030002_01.dmp');

COMMIT;