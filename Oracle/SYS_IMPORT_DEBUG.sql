CREATE TABLE SYS.IMPORT_DEBUG
(
  SQL_STATEMENT  VARCHAR2(4000 BYTE),
  SERVICE_ID     VARCHAR2(64 BYTE),
  REQ_INPUTS     NUMBER,
  DATE_TIME      DATE                           DEFAULT SYSDATE,
  COMMENTS       VARCHAR2(1000 BYTE),
  SEQ            NUMBER
);

CREATE OR REPLACE PUBLIC SYNONYM IMPORT_DEBUG FOR SYS.IMPORT_DEBUG;