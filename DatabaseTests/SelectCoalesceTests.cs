using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class SelectCoalesceTests
    {
        public class LookupEntity
        {
            public string Key { get; set; }
            public string Value { get; set; }
            public string DisplayValue { get; set; }
        }

        [TestInitialize]
        public void InitializeDb()
        {
            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
        }

        [DataTestMethod]
        [DataRow( null, 3 )]
        [DataRow( "EPSG", 3 )]
        public void SelectCoalesce_FilledValue_ShouldNotBeReplaced( string key, int count )
        {
            const string sqlInsert = @"
INSERT [dbo].[Lookup] ([Key], [Value], [DisplayValue] ) VALUES (N'EPSG', N'EPSG:3857' , N'WGS 84 / Pseudo-Mercator')
INSERT [dbo].[Lookup] ([Key], [Value], [DisplayValue] ) VALUES (N'EPSG', N'EPSG:4230' , N'ED50 / Longitude/latitude')
INSERT [dbo].[Lookup] ([Key], [Value], [DisplayValue] ) VALUES (N'EPSG', N'EPSG:4258' , N'ETRS89 / Longitude/latitude')
";
            const string sqlCreate = @"
CREATE TABLE [dbo].[Lookup]
(
	[Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY, 
    [Key] NVARCHAR(50) NOT NULL, 
    [Value] NVARCHAR(50) NOT NULL, 
    [DisplayValue] NVARCHAR(MAX) NULL
)";

            const string sqlSelect = @"
SELECT [Key]
     , [Value]
     , COALESCE([DisplayValue],[Value]) as [DisplayValue]
FROM [dbo].[Lookup]
WHERE @key IS NULL OR [Key] = @key
";

            using var connection = new MemoryDbConnection();
            connection.Execute( sqlCreate );
            connection.Execute( sqlInsert );

            var result = connection.Query<LookupEntity>( sqlSelect, new { key } ).ToList();
            result.Count.Should().Be( count );
        }

        [TestMethod]
        public void SelectCoalesce_EmptyValue_ShouldBeReplaced( )
        {
            const string sqlInsert = @"INSERT [dbo].[Lookup] ([Key], [Value], [DisplayValue] ) VALUES (N'EPSG', N'EPSG:3857', NULL)";
            const string sqlCreate = @"
CREATE TABLE [dbo].[Lookup]
(
	[Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY, 
    [Key] NVARCHAR(50) NOT NULL, 
    [Value] NVARCHAR(50) NOT NULL, 
    [DisplayValue] NVARCHAR(MAX) NULL
)";

            const string sqlSelect = @"
SELECT [Key]
     , [Value]
     , COALESCE([DisplayValue],[Value]) as [DisplayValue]
FROM [dbo].[Lookup]
";
            using var connection = new MemoryDbConnection();
            connection.Execute( sqlCreate );
            connection.Execute( sqlInsert );

            var result = connection.QuerySingle<LookupEntity>( sqlSelect );
            result.DisplayValue.Should().Be( result.Value );
        }

        [TestMethod]
        public void SelectCoalesce_EmptyValue2_ShouldBeReplaced()
        {
            const string sqlInsert = @"INSERT [dbo].[Lookup] ([Key], [Value] ) VALUES (N'EPSG', N'EPSG:3857')";
            const string sqlCreate = @"
CREATE TABLE [dbo].[Lookup]
(
	[Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY, 
    [Key] NVARCHAR(50) NOT NULL, 
    [Value] NVARCHAR(50) NOT NULL, 
    [DisplayValue] NVARCHAR(MAX) NULL
)";

            const string sqlSelect = @"
SELECT [Key]
     , [Value]
     , COALESCE([DisplayValue],[Value]) as [DisplayValue]
FROM [dbo].[Lookup]
";

            using var connection = new MemoryDbConnection();
            connection.Execute( sqlCreate );
            connection.Execute( sqlInsert );

            var result = connection.QuerySingle<LookupEntity>( sqlSelect );
            result.DisplayValue.Should().Be( result.Value );
        }

        [TestMethod]
        public void SelectCoalesce_EmptyValue3_ShouldBeReplaced()
        {
            const string sqlInsert = @"INSERT [dbo].[Lookup] ([Key], [Value], [DisplayValue] ) VALUES (@Key, @Value, @DisplayValue)";
            const string sqlCreate = @"
CREATE TABLE [dbo].[Lookup]
(
	[Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY, 
    [Key] NVARCHAR(50) NOT NULL, 
    [Value] NVARCHAR(50) NOT NULL, 
    [DisplayValue] NVARCHAR(MAX) NULL
)";

            const string sqlSelect = @"
SELECT [Key]
     , [Value]
     , COALESCE([DisplayValue],[Value]) as [DisplayValue]
FROM [dbo].[Lookup]
";

            var newLookup = new LookupEntity {Key = "New", Value = "New Value" };
            using var connection = new MemoryDbConnection();
            connection.Execute( sqlCreate );
            connection.Execute( sqlInsert, newLookup );

            var result = connection.QuerySingle<LookupEntity>( sqlSelect );
            result.DisplayValue.Should().Be( result.Value );
        }


    }
}
