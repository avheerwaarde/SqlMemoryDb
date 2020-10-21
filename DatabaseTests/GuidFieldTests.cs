using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class GuidFieldTests
    {
        class TestDefaults
        {
            public int Id { get; set; }
            public Guid ETag { get; set; }
            public bool IsDeleted { get; set; }
            public DateTime CreatedAt { get; set; }
            public string Name { get; set; }
        }

        private const string _SqlSqlCreateTable = @"
CREATE TABLE [dbo].[TestDefaults]
(
    [Id]			INT IDENTITY (1, 1) NOT NULL ,
    [ETag]			UNIQUEIDENTIFIER  CONSTRAINT [DF_Project_ETag] DEFAULT (newid()) NOT NULL, 
    [IsDeleted]		BIT            CONSTRAINT [DF_Project_IsDeleted] DEFAULT ((0)) NOT NULL,
    [CreatedAt]		DATETIME       CONSTRAINT [DF_Project_CreatedAt] DEFAULT (getutcdate()) NOT NULL,
    [Name]          NVARCHAR(MAX) NOT NULL
)";

        [TestMethod]
        public void CreateTable_WithDefaults_RowIsCreated()
        {
            const string sqlInsert = "INSERT INTO TestDefaults ([Name]) VALUES (N'New name')";

            using var connection = new MemoryDbConnection();
            var db = connection.GetMemoryDatabase();
            db.Tables.Clear();
            connection.Execute( _SqlSqlCreateTable );
            db.Tables[ "dbo.TestDefaults" ].Columns[ 1 ].HasDefault.Should().BeTrue();
            db.Tables[ "dbo.TestDefaults" ].Columns[ 1 ].DefaultCallExpression.Should().NotBeNull(  );
            db.Tables[ "dbo.TestDefaults" ].Columns[ 2 ].HasDefault.Should().BeTrue();
            db.Tables[ "dbo.TestDefaults" ].Columns[ 2 ].DefaultValue.Should().NotBeNullOrWhiteSpace(  );
            db.Tables[ "dbo.TestDefaults" ].Columns[ 3 ].HasDefault.Should().BeTrue();
            db.Tables[ "dbo.TestDefaults" ].Columns[ 3 ].DefaultCallExpression.Should().NotBeNull();

            connection.Execute( sqlInsert );
            var insertedRow = connection.QueryFirst<TestDefaults>( "SELECT Id, ETag, IsDeleted, CreatedAt, Name from TestDefaults" );
            insertedRow.Id.Should().NotBe( 0 );
            insertedRow.ETag.Should().NotBe( Guid.Empty );
            insertedRow.IsDeleted.Should().BeFalse();
            insertedRow.CreatedAt.Should().BeCloseTo( DateTime.UtcNow, 1000 );
            insertedRow.Name.Should().Be( "New name" );
        }

    }
}
