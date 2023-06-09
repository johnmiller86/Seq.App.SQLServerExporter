﻿namespace Seq.Apps.SQLServerExporter
{
    internal class Scripts
    {
        internal static string CreateSchema = @"
            IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @SchemaName)
            BEGIN
	            DECLARE @SQL VARCHAR(MAX) = 'CREATE SCHEMA ' + QUOTENAME(@SchemaName) + ' AUTHORIZATION [dbo]';
	            EXEC(@SQL);
            END";

        internal static string CreateTable = @"
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @SchemaName AND TABLE_NAME = @TableName)
            BEGIN
	            DECLARE @SQL VARCHAR(MAX) = '
	            CREATE TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' (
		            [EventLogId] BIGINT PRIMARY KEY IDENTITY (1, 1),
		            [SeqEventId] NVARCHAR(50) NOT NULL,
		            [SeqEventIngestionTimestamp] NVARCHAR(30) NOT NULL,
                    [SeqEventLocalTimestamp] NVARCHAR(30) NOT NULL,
                    [SeqEventLevel] NVARCHAR(15) NOT NULL,
                    [SeqEventMessage] NVARCHAR(MAX) NOT NULL,
                    [SeqEventPropertiesJSON] NVARCHAR(MAX) NOT NULL
	            )';
	            EXEC(@SQL);
            END";

        internal static string InsertEvent = @"
            DECLARE @params NVARCHAR(100) = '@S NVARCHAR(128), @T NVARCHAR(128), @C NVARCHAR(MAX), @V NVARCHAR(MAX)';
            DECLARE @S NVARCHAR(128) = @SchemaName;
            DECLARE @T NVARCHAR(128)= @TableName;
            DECLARE @C NVARCHAR(MAX) = @Columns;
            DECLARE @V NVARCHAR(MAX) = @Values;
            DECLARE @SQL NVARCHAR(MAX) = 'INSERT INTO ' + QUOTENAME(@S) + '.' + QUOTENAME(@T) + '(' + @C + ') VALUES(' + @V + ')';
            EXEC sp_executesql @SQL, @params, @S = @SchemaName, @T = @TableName, @C = @Columns, @V = @Values;";
    }
}