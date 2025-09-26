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

/* -- Pull column + PK metadata from DB1 -- */
SET @sql = N'
INSERT INTO #cols1(col_name,is_key,data_type,is_computed,is_rowversion)
SELECT c.name,
       CASE WHEN pk.col_name IS NULL THEN 0 ELSE 1 END,
       t.name,
       c.is_computed,
       CASE WHEN t.name IN (''timestamp'',''rowversion'') THEN 1 ELSE 0 END
FROM ['+QUOTENAME(@DB1)+N'].sys.columns c
JOIN ['+QUOTENAME(@DB1)+N'].sys.tables  tb ON tb.object_id=c.object_id
JOIN ['+QUOTENAME(@DB1)+N'].sys.schemas sc ON sc.schema_id=tb.schema_id
JOIN ['+QUOTENAME(@DB1)+N'].sys.types   t  ON t.user_type_id=c.user_type_id
OUTER APPLY (
  SELECT c2.name AS col_name
  FROM ['+QUOTENAME(@DB1)+N'].sys.indexes i
  JOIN ['+QUOTENAME(@DB1)+N'].sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
  JOIN ['+QUOTENAME(@DB1)+N'].sys.columns c2 ON c2.object_id=ic.object_id AND c2.column_id=ic.column_id
  WHERE i.is_primary_key=1 AND i.object_id=tb.object_id AND c2.column_id=c.column_id
) pk
WHERE tb.name=@t AND sc.name=@s;
';
EXEC sp_executesql @sql, N'@s sysname,@t sysname', @Schema1, @Table1;

/* -- Pull column + PK metadata from DB2 -- */
SET @sql = N'
INSERT INTO #cols2(col_name,is_key,data_type,is_computed,is_rowversion)
SELECT c.name,
       CASE WHEN pk.col_name IS NULL THEN 0 ELSE 1 END,
       t.name,
       c.is_computed,
       CASE WHEN t.name IN (''timestamp'',''rowversion'') THEN 1 ELSE 0 END
FROM ['+QUOTENAME(@DB2)+N'].sys.columns c
JOIN ['+QUOTENAME(@DB2)+N'].sys.tables  tb ON tb.object_id=c.object_id
JOIN ['+QUOTENAME(@DB2)+N'].sys.schemas sc ON sc.schema_id=tb.schema_id
JOIN ['+QUOTENAME(@DB2)+N'].sys.types   t  ON t.user_type_id=c.user_type_id
OUTER APPLY (
  SELECT c2.name AS col_name
  FROM ['+QUOTENAME(@DB2)+N'].sys.indexes i
  JOIN ['+QUOTENAME(@DB2)+N'].sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
  JOIN ['+QUOTENAME(@DB2)+N'].sys.columns c2 ON c2.object_id=ic.object_id AND c2.column_id=ic.column_id
  WHERE i.is_primary_key=1 AND i.object_id=tb.object_id AND c2.column_id=c.column_id
) pk
WHERE tb.name=@t AND sc.name=@s;
';
EXEC sp_executesql @sql, N'@s sysname,@t sysname', @Schema2, @Table2;

/* -- Resolve key list (user-supplied or PK) -- */
IF @KeyList IS NULL OR LTRIM(RTRIM(@KeyList))=N''
BEGIN
  INSERT INTO #keys1 SELECT col_name FROM #cols1 WHERE is_key=1;
  INSERT INTO #keys2 SELECT col_name FROM #cols2 WHERE is_key=1;
  IF NOT EXISTS (SELECT 1 FROM #keys1)
  BEGIN RAISERROR('No PK detected; pass @KeyList (comma-separated).',16,1); RETURN; END;

  /* Ensure both sides share same key set */
  IF EXISTS (SELECT 1 FROM (SELECT col_name FROM #keys1 EXCEPT SELECT col_name FROM #keys2) x
             UNION ALL
             SELECT 1 FROM (SELECT col_name FROM #keys2 EXCEPT SELECT col_name FROM #keys1) y)
  BEGIN RAISERROR('Primary keys differ; pass @KeyList explicitly.',16,1); RETURN; END
END
ELSE
BEGIN
  ;WITH s AS (SELECT LTRIM(RTRIM(value)) col_name FROM STRING_SPLIT(@KeyList, ','))
  INSERT INTO #keys1 SELECT col_name FROM s;
  ;WITH s AS (SELECT LTRIM(RTRIM(value)) col_name FROM STRING_SPLIT(@KeyList, ','))
  INSERT INTO #keys2 SELECT col_name FROM s;
END

/* -- Comparable column intersection (exclude keys, computed, rowversion) -- */
CREATE TABLE #cmp(col_name sysname PRIMARY KEY, type1 nvarchar(128), type2 nvarchar(128), is_textlike bit, is_binary bit);

INSERT INTO #cmp(col_name,type1,type2,is_textlike,is_binary)
SELECT c1.col_name,
       c1.data_type, c2.data_type,
       CASE WHEN c1.data_type IN (N'char',N'nchar',N'varchar',N'nvarchar',N'text',N'ntext')
          AND c2.data_type IN (N'char',N'nchar',N'varchar',N'nvarchar',N'text',N'ntext') THEN 1 ELSE 0 END,
       CASE WHEN c1.data_type IN (N'varbinary',N'binary',N'image',N'timestamp',N'rowversion')
          OR  c2.data_type IN (N'varbinary',N'binary',N'image',N'timestamp',N'rowversion') THEN 1 ELSE 0 END
FROM #cols1 c1
JOIN #cols2 c2 ON c2.col_name=c1.col_name
WHERE c1.col_name NOT IN (SELECT col_name FROM #keys1)
  AND c2.col_name NOT IN (SELECT col_name FROM #keys2)
  AND c1.is_computed=0 AND c2.is_computed=0
  AND c1.is_rowversion=0 AND c2.is_rowversion=0;

/* ==== Build dynamic compare statements ==== */
DECLARE 
  @QKeys     nvarchar(MAX),
  @QJoin     nvarchar(MAX),
  @QSelect   nvarchar(MAX),
  @QDiffCols nvarchar(MAX) = N'',
  @QWhere    nvarchar(MAX) = N'';

SELECT @QKeys = STRING_AGG(QUOTENAME(col_name), N',') FROM #keys1;
SELECT @QJoin = STRING_AGG(N'A.'+QUOTENAME(col_name)+N'=B.'+QUOTENAME(col_name), N' AND ') FROM #keys1;

SELECT @QSelect = STRING_AGG(
  CASE WHEN is_binary=1 THEN
    N'CONVERT(varchar(514),sys.fn_varbintohexstr(A.'+QUOTENAME(col_name)+N')) AS '+QUOTENAME(col_name+N'_DB1')+N','+
    N'CONVERT(varchar(514),sys.fn_varbintohexstr(B.'+QUOTENAME(col_name)+N')) AS '+QUOTENAME(col_name+N'_DB2')
  ELSE
    N'A.'+QUOTENAME(col_name)+N' AS '+QUOTENAME(col_name+N'_DB1')+N','+
    N'B.'+QUOTENAME(col_name)+N' AS '+QUOTENAME(col_name+N'_DB2')
  END
, N',')
FROM #cmp;

SELECT @QDiffCols = STRING_AGG(
  CASE 
    WHEN is_binary=1 THEN
      N'ISNULL(CONVERT(varchar(514),sys.fn_varbintohexstr(A.'+QUOTENAME(col_name)+N')),N''<NULL>'') <> '+
      N'ISNULL(CONVERT(varchar(514),sys.fn_varbintohexstr(B.'+QUOTENAME(col_name)+N')),N''<NULL>'')'
    WHEN @CaseInsensitive=1 AND is_textlike=1 THEN
      N'ISNULL(CONVERT(nvarchar(max),A.'+QUOTENAME(col_name)+N') COLLATE Latin1_General_CI_AI,N''<NULL>'') <> '+
      N'ISNULL(CONVERT(nvarchar(max),B.'+QUOTENAME(col_name)+N') COLLATE Latin1_General_CI_AI,N''<NULL>'')'
    ELSE
      N'ISNULL(CONVERT(nvarchar(max),A.'+QUOTENAME(col_name)+N'),N''<NULL>'') <> '+
      N'ISNULL(CONVERT(nvarchar(max),B.'+QUOTENAME(col_name)+N'),N''<NULL>'')'
  END
, N' OR ')
FROM #cmp;

IF @ReturnOnlyDiffs=1 AND COALESCE(@QDiffCols,N'')<>N''
  SET @QWhere = N'WHERE ('+@QDiffCols+N') OR A.'+REPLACE(@QKeys, N'],[', N'] IS NULL OR B[')+N'] IS NULL';

/* ==== Quick counts (sanity) ==== */
DECLARE @countSql nvarchar(MAX) = N'
SELECT
  (SELECT COUNT(*) FROM ['+@DB1+N'].'+QUOTENAME(@Schema1)+N'.'+QUOTENAME(@Table1)+N') AS Count_DB1,
  (SELECT COUNT(*) FROM ['+@DB2+N'].'+QUOTENAME(@Schema2)+N'.'+QUOTENAME(@Table2)+N') AS Count_DB2;';
SET @countSql = REPLACE(@countSql, N'@Schema1', @Schema1);
SET @countSql = REPLACE(@countSql, N'@Table1',  @Table1);
SET @countSql = REPLACE(@countSql, N'@Schema2', @Schema2);
SET @countSql = REPLACE(@countSql, N'@Table2',  @Table2);
EXEC (@countSql);

/* ==== Result 1: keys only in DB1 ==== */
DECLARE @only1 nvarchar(MAX) = N'
SELECT '+@QKeys+N'
FROM ['+@DB1+N'].'+QUOTENAME(@Schema1)+N'.'+QUOTENAME(@Table1)+N' A
LEFT JOIN ['+@DB2+N'].'+QUOTENAME(@Schema2)+N'.'+QUOTENAME(@Table2)+N' B
  ON '+@QJoin+N'
WHERE '+REPLACE(@QKeys, N'],[', N'] IS NULL AND B[')+N'] IS NULL;';
SET @only1 = REPLACE(@only1, N'@Schema1', @Schema1);
SET @only1 = REPLACE(@only1, N'@Table1',  @Table1);
SET @only1 = REPLACE(@only1, N'@Schema2', @Schema2);
SET @only1 = REPLACE(@only1, N'@Table2',  @Table2);
EXEC (@only1);

/* ==== Result 2: keys only in DB2 ==== */
DECLARE @only2 nvarchar(MAX) = N'
SELECT '+@QKeys+N'
FROM ['+@DB2+N'].'+QUOTENAME(@Schema2)+N'.'+QUOTENAME(@Table2)+N' B
LEFT JOIN ['+@DB1+N'].'+QUOTENAME(@Schema1)+N'.'+QUOTENAME(@Table1)+N' A
  ON '+@QJoin+N'
WHERE '+REPLACE(@QKeys, N'],[', N'] IS NULL AND A[')+N'] IS NULL;';
SET @only2 = REPLACE(@only2, N'@Schema1', @Schema1);
SET @only2 = REPLACE(@only2, N'@Table1',  @Table1);
SET @only2 = REPLACE(@only2, N'@Schema2', @Schema2);
SET @only2 = REPLACE(@only2, N'@Table2',  @Table2);
EXEC (@only2);

/* ==== Result 3: mismatched rows with side-by-side values ==== */
IF @QSelect IS NULL OR LEN(@QSelect)=0
BEGIN
  -- No comparable non-key columns; show only missing keys (results #1 & #2 already returned)
  SELECT 'No comparable non-key columns (or all excluded).' AS InfoMsg;
END
ELSE
BEGIN
  DECLARE @mismatch nvarchar(MAX) = N'
  SELECT '+@QKeys+N','+@QSelect+N'
  FROM ['+@DB1+N'].'+QUOTENAME(@Schema1)+N'.'+QUOTENAME(@Table1)+N' A
  FULL OUTER JOIN ['+@DB2+N'].'+QUOTENAME(@Schema2)+N'.'+QUOTENAME(@Table2)+N' B
    ON '+@QJoin+N'
  '+CASE WHEN @QWhere=N'' THEN N'' ELSE @QWhere END+N';';
  SET @mismatch = REPLACE(@mismatch, N'@Schema1', @Schema1);
  SET @mismatch = REPLACE(@mismatch, N'@Table1',  @Table1);
  SET @mismatch = REPLACE(@mismatch, N'@Schema2', @Schema2);
  SET @mismatch = REPLACE(@mismatch, N'@Table2',  @Table2);
  EXEC (@mismatch);
END
