using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class WhereTests
    {
        public class LookupEntity
        {
            public string Key { get; set; }
            public string Value { get; set; }
            public string DisplayValue { get; set; }
            public string DisplayValue33 { get; set; }
        }

        [TestInitialize]
        public void InitializeDb()
        {
            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
        }

        [DataTestMethod]
        [DataRow( null, 3 )]
        [DataRow( "Name1", 1 )]
        [DataRow( "Name2", 1 )]
        [DataRow( "Not found", 0 )]
        public void WhereClause_OptionalTest_ShouldFilter( string name, int count )
        {
            var applications = new ApplicationDto[]
            {
                new ApplicationDto { Name = "Name1", User = "User1", DefName = "Def1"},
                new ApplicationDto { Name = "Name2", User = "User2", DefName = "Def2"},
                new ApplicationDto { Name = "Name3", User = "User3", DefName = "Def3"},
            };
            const string sqlInsert = "INSERT INTO application ([Name], [User], [DefName]) VALUES (@Name, @User, @DefName)";

            using var connection = new MemoryDbConnection();
            connection.Execute( SqlStatements.SqlCreateTableApplication );
            connection.Execute( sqlInsert, applications );

            string sqlSelect = $"{SqlStatements.SqlSelectApplication} WHERE @name is NULL OR @name = [Name]";
            var appsQueried = connection.Query<ApplicationDto>( sqlSelect, new { name = name } ).ToList();
            appsQueried.Count.Should().Be( count );
            if ( count == 1 )
            {
                appsQueried.First().Name.Should().Be( name );
            }
        }

        [DataTestMethod]
        [DataRow( null, 3 )]
        [DataRow( "EPSG", 3 )]
        public void WhereClause_Lookup_ShouldNotFail( string key, int count )
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
     , COALESCE([DisplayValue],[Value]) as [DisplayValue33]
FROM [dbo].[Lookup]
WHERE @key IS NULL OR [Key] = @key
";

            using var connection = new MemoryDbConnection();
            connection.Execute( sqlCreate );
            connection.Execute( sqlInsert );

            var result = connection.Query<LookupEntity>( sqlSelect, new { key } ).ToList();
            result.Count.Should().Be( count );
        }

    }
}
