syntax case ignore

syntax keyword simpleKeyword ADD EXTERNAL PROCEDURE ALL FETCH PUBLIC ALTER FILE RAISERROR AND FILLFACTOR READ ANY FOR READTEXT AS FOREIGN RECONFIGURE ASC FREETEXT REFERENCES AUTHORIZATION FREETEXTTABLE REPLICATION BACKUP FROM RESTORE BEGIN FULL RESTRICT BETWEEN FUNCTION RETURN BREAK GOTO REVERT BROWSE GRANT REVOKE BULK GROUP RIGHT BY HAVING ROLLBACK CASCADE HOLDLOCK ROWCOUNT CASE IDENTITY ROWGUIDCOL CHECK IDENTITY_INSERT RULE CHECKPOINT IDENTITYCOL SAVE CLOSE IF SCHEMA CLUSTERED IN SECURITYAUDIT COALESCE INDEX SELECT COLLATE INNER SEMANTICKEYPHRASETABLE COLUMN INSERT SEMANTICSIMILARITYDETAILSTABLE COMMIT INTERSECT SEMANTICSIMILARITYTABLE COMPUTE INTO SESSION_USER CONSTRAINT IS SET JOIN SETUSER CONTAINSTABLE KEY SHUTDOWN CONTINUE KILL SOME CONVERT LEFT STATISTICS CREATE LIKE SYSTEM_USER CROSS LINENO TABLE CURRENT LOAD TABLESAMPLE CURRENT_DATE MERGE TEXTSIZE CURRENT_TIME NATIONAL THEN CURRENT_TIMESTAMP NOCHECK TO CURRENT_USER NONCLUSTERED TOP CURSOR NOT TRAN DATABASE NULL TRANSACTION DBCC NULLIF TRIGGER DEALLOCATE OF TRUNCATE DECLARE OFF TRY_CONVERT DEFAULT OFFSETS TSEQUAL DELETE ON UNION DENY OPEN UNIQUE DESC OPENDATASOURCE UNPIVOT DISK OPENQUERY UPDATE DISTINCT OPENROWSET UPDATETEXT DISTRIBUTED OPENXML USE DOUBLE OPTION USER DROP OR VALUES DUMP ORDER VARYING ELSE OUTER VIEW END OVER WAITFOR ERRLVL PERCENT WHEN ESCAPE PIVOT WHERE EXCEPT PLAN WHILE EXEC PRECISION WITH EXECUTE PRIMARY WITHIN GROUP EXISTS PRINT WRITETEXT EXIT PROC 
syntax keyword simpleKeyword CONTAINS
syntax keyword simpleKeyword MAX
syntax keyword advancedKeyword GO

syntax keyword tsqlType bigint binary bit char date datetime datetime2 datetimeoffset decimal float geography geometry hierarchyid image int money nchar ntext numeric nvarchar real smalldatetime smallint smallmoney sql_variant sysname text time timestamp tinyint uniqueidentifier varbinary varchar xml

syntax region tsqlString start=+'+ skip=+''+ end=+'+ keepend extend

syntax match simpleComment "--.*$"
syntax region multiComment start=/\/\*/ end=/\*\// contains=sqlComment

syntax match specialKeyword /@@\w\+/
syntax match tsqlVariable /#\+\w\+/
syntax match tsqlVariable /@\w\+/ 
syntax match tsqlNumber /\v<(\d+(\.\d*)?|\.\d+)>/

highlight def link tsqlNumber Number
highlight def link simpleKeyword Keyword
highlight def link advancedKeyword Keyword
highlight def link simpleComment Comment
highlight def link multiComment Comment
highlight def link tsqlString String
highlight def link specialKeyword Special
highlight def link tsqlType Type
highlight def link tsqlVariable Identifier
