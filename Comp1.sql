/* ==== CONFIG: set your targets here ==== */
DECLARE 
  @DB1     sysname       = N'SourceDB',
  @Schema1 sysname       = N'dbo',
  @Table1  sysname       = N'YourTable',
  @DB2     sysname       = N'TargetDB',
  @Schema2 sysname       = N'dbo',
  @Table2  sysname       = N'YourTable',
  @KeyList nvarchar(MAX) = NULL,       -- e.g., N'Id,Code' ; NULL = use PK
  @CaseInsensitive bit   = 1,          -- 1 = ignore case for text
  @ReturnOnlyDiffs bit   = 1;          -- 1 = only mismatched rows in result #3

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#cols1') IS NOT NULL DROP TABLE #cols1;
IF OBJECT_ID('tempdb..#cols2') IS NOT NULL DROP TABLE #cols2;
IF OBJECT_ID('tempdb..#keys1') IS NOT NULL DROP TABLE #keys1;
IF OBJECT_ID('tempdb..#keys2') IS NOT NULL DROP TABLE #keys2;
IF OBJECT_ID('tempdb..#cmp')   IS NOT NULL DROP TABLE #cmp;

CREATE TABLE #cols1(col_name sysname, is_key bit, data_type nvarchar(128), is_computed bit, is_rowversion bit);
CREATE TABLE #cols2(col_name sysname, is_key bit, data_type nvarchar(128), is_computed bit, is_rowversion bit);
CREATE TABLE #keys1(col_name sysname PRIMARY KEY);
CREATE TABLE #keys2(col_name sysname PRIMARY KEY);

DECLARE @sql nvarchar(MAX);

/* --- Collect metadata from DB1 --- */
SET @sql = N'
INSERT INTO #cols1(col_name,is_key,data_type,is_computed,is_rowversion)
SELECT c.name,
       CASE WHEN pk.col_name IS NULL THEN 0 ELSE 1 END,
       t.name,
       c.is_computed,
       CASE WHEN t.name IN (''timestamp'',''rowversion'') THEN 1 ELSE 0 END
FROM ['+@DB1+N'].sys.columns c
JOIN ['+@DB1+N'].sys.tables tb ON tb.object_id=c.object_id
JOIN ['+@DB1+N'].sys.schemas sc ON sc.schema_id=tb.schema_id
JOIN ['+@DB1+N'].sys.types t ON t.user_type_id=c.user_type_id
OUTER APPLY (
    SELECT c2.name AS col_name
    FROM ['+@DB1+N'].sys.indexes i
    JOIN ['+@DB1+N'].sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
    JOIN ['+@DB1+N'].sys.columns c2 ON c2.object_id=ic.object_id AND c2.column_id=ic.column_id
    WHERE i.is_primary_key=1 AND i.object_id=tb.object_id AND c2.column_id=c.column_id
) pk
WHERE tb.name=@t AND sc.name=@s;
';
EXEC sp_executesql @sql, N'@s sysname,@t sysname', @Schema1, @Table1;

/* --- Collect metadata from DB2 --- */
SET @sql = REPLACE(@sql,@DB1,@DB2);
EXEC sp_executesql @sql, N'@s sysname,@t sysname', @Schema2, @Table2;

/* --- Derive key list --- */
IF @KeyList IS NULL OR LTRIM(RTRIM(@KeyList))=N''
BEGIN
  INSERT INTO #keys1 SELECT col_name FROM #cols1 WHERE is_key=1;
  INSERT INTO #keys2 SELECT col_name FROM #cols2 WHERE is_key=1;
  IF NOT EXISTS (SELECT 1 FROM #keys1)
  BEGIN RAISERROR('No PK detected; pass @KeyList.',16,1); RETURN; END;
  IF EXISTS (SELECT 1 FROM (SELECT col_name FROM #keys1 EXCEPT SELECT col_name FROM #keys2)x
             UNION ALL
             SELECT 1 FROM (SELECT col_name FROM #keys2 EXCEPT SELECT col_name FROM #keys1)y)
  BEGIN RAISERROR('Primary keys differ; pass @KeyList explicitly.',16,1); RETURN; END;
END
ELSE
BEGIN
  ;WITH s AS (SELECT LTRIM(RTRIM(value)) col_name FROM STRING_SPLIT(@KeyList,','))
  INSERT INTO #keys1 SELECT col_name FROM s;
  ;WITH s AS (SELECT LTRIM(RTRIM(value)) col_name FROM STRING_SPLIT(@KeyList,','))
  INSERT INTO #keys2 SELECT col_name FROM s;
END;

/* --- Comparable column intersection --- */
CREATE TABLE #cmp(col_name sysname PRIMARY KEY, type1 nvarchar(128), type2 nvarchar(128), is_textlike bit, is_binary bit);

INSERT INTO #cmp(col_name,type1,type2,is_textlike,is_binary)
SELECT c1.col_name,
       c1.data_type,c2.data_type,
       CASE WHEN c1.data_type LIKE '%char%' OR c1.data_type LIKE '%text%' THEN 1 ELSE 0 END,
       CASE WHEN c1.data_type IN(N'varbinary',N'binary',N'image',N'timestamp',N'rowversion')
          OR c2.data_type IN(N'varbinary',N'binary',N'image',N'timestamp',N'rowversion') THEN 1 ELSE 0 END
FROM #cols1 c1
JOIN #cols2 c2 ON c1.col_name=c2.col_name
WHERE c1.col_name NOT IN (SELECT col_name FROM #keys1)
  AND c2.col_name NOT IN (SELECT col_name FROM #keys2)
  AND c1.is_computed=0 AND c2.is_computed=0
  AND c1.is_rowversion=0 AND c2.is_rowversion=0;

/* ==== Build dynamic compare lists (FOR XML instead of STRING_AGG) ==== */
DECLARE @QKeys nvarchar(MAX), @QJoin nvarchar(MAX), @QSelect nvarchar(MAX), @QDiffCols nvarchar(MAX), @QWhere nvarchar(MAX)='';

/* Key list */
SELECT @QKeys = STUFF((SELECT ','+QUOTENAME(col_name) FROM #keys1 FOR XML PATH(''),TYPE).value('.','NVARCHAR(MAX)'),1,1,'');

/* Join condition */
SELECT @QJoin = STUFF((SELECT ' AND A.'+QUOTENAME(col_name)+'=B.'+QUOTENAME(col_name)
                       FROM #keys1 FOR XML PATH(''),TYPE).value('.','NVARCHAR(MAX)'),1,5,'');

/* Column pairs for select */
SELECT @QSelect = STUFF((SELECT ','+CASE WHEN is_binary=1 THEN
       'CONVERT(varchar(514),sys.fn_varbintohexstr(A.'+QUOTENAME(col_name)+')) AS '+QUOTENAME(col_name+'_DB1')+','+
       'CONVERT(varchar(514),sys.fn_varbintohexstr(B.'+QUOTENAME(col_name)+')) AS '+QUOTENAME(col_name+'_DB2')
     ELSE
       'A.'+QUOTENAME(col_name)+' AS '+QUOTENAME(col_name+'_DB1')+','+
       'B.'+QUOTENAME(col_name)+' AS '+QUOTENAME(col_name+'_DB2')
     END
FROM #cmp FOR XML PATH(''),TYPE).value('.','NVARCHAR(MAX)'),1,1,'');

/* Diff condition */
SELECT @QDiffCols = STUFF((SELECT ' OR '+
     CASE WHEN is_binary=1 THEN
       'ISNULL(CONVERT(varchar(514),sys.fn_varbintohexstr(A.'+QUOTENAME(col_name)+')),''<NULL>'') <> ISNULL(CONVERT(varchar(514),sys.fn_varbintohexstr(B.'+QUOTENAME(col_name)+')),''<NULL>'')'
     WHEN @CaseInsensitive=1 AND is_textlike=1 THEN
       'ISNULL(CONVERT(nvarchar(max),A.'+QUOTENAME(col_name)+') COLLATE Latin1_General_CI_AI,''<NULL>'') <> ISNULL(CONVERT(nvarchar(max),B.'+QUOTENAME(col_name)+') COLLATE Latin1_General_CI_AI,''<NULL>'')'
     ELSE
       'ISNULL(CONVERT(nvarchar(max),A.'+QUOTENAME(col_name)+'),''<NULL>'') <> ISNULL(CONVERT(nvarchar(max),B.'+QUOTENAME(col_name)+'),''<NULL>'')'
     END
FROM #cmp FOR XML PATH(''),TYPE).value('.','NVARCHAR(MAX)'),1,4,'');

IF @ReturnOnlyDiffs=1 AND COALESCE(@QDiffCols,'')<>'' 
    SET @QWhere = 'WHERE ('+@QDiffCols+') OR A.'+REPLACE(@QKeys,'],[','] IS NULL OR B[')+'] IS NULL';

/* ==== Quick counts ==== */
EXEC('SELECT 
 (SELECT COUNT(*) FROM ['+@DB1+'].'+@Schema1+'.'+@Table1+') AS Count_DB1,
 (SELECT COUNT(*) FROM ['+@DB2+'].'+@Schema2+'.'+@Table2+') AS Count_DB2;');

/* ==== Result 1: only in DB1 ==== */
EXEC('SELECT '+@QKeys+' 
FROM ['+@DB1+'].'+@Schema1+'.'+@Table1+' A
LEFT JOIN ['+@DB2+'].'+@Schema2+'.'+@Table2+' B ON '+@QJoin+'
WHERE '+REPLACE(@QKeys,'],[','] IS NULL AND B[')+'] IS NULL;');

/* ==== Result 2: only in DB2 ==== */
EXEC('SELECT '+@QKeys+'
FROM ['+@DB2+'].'+@Schema2+'.'+@Table2+' B
LEFT JOIN ['+@DB1+'].'+@Schema1+'.'+@Table1+' A ON '+@QJoin+'
WHERE '+REPLACE(@QKeys,'],[','] IS NULL AND A[')+'] IS NULL;');

/* ==== Result 3: mismatched rows ==== */
IF COALESCE(@QSelect,'')=''
    PRINT 'No comparable non-key columns.'
ELSE
    EXEC('SELECT '+@QKeys+','+@QSelect+'
          FROM ['+@DB1+'].'+@Schema1+'.'+@Table1+' A
          FULL OUTER JOIN ['+@DB2+'].'+@Schema2+'.'+@Table2+' B ON '+@QJoin+'
          '+@QWhere);
