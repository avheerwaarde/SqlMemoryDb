using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class TimeStampTests
    {
        public const string SqlCreateTableSessionToken = @"
CREATE TABLE [dbo].[SessionToken]
(
	[FkUser] INT NOT NULL PRIMARY KEY,
	[SessionToken] NVARCHAR(MAX) NOT NULL, 
    [TimeStamp] TIMESTAMP NOT NULL
)";

        public const string SqlCreateTableUser = @"
CREATE TABLE [dbo].[User]
(
    [Id]			INT IDENTITY (1, 1) NOT NULL ,
    [ETag]			UNIQUEIDENTIFIER  CONSTRAINT [DF_User_ETag] DEFAULT (newid()) NOT NULL, 
    [IsDeleted]		BIT            CONSTRAINT [DF_User_IsDeleted] DEFAULT ((0)) NOT NULL,
    [CreatedAt]		DATETIME       CONSTRAINT [DF_User_CreatedAt] DEFAULT (getutcdate()) NOT NULL,
    [UpdatedAt]		DATETIME       CONSTRAINT [DF_User_UpdatedAt] DEFAULT (getutcdate()) NOT NULL, 
    [UpdatedBy]     INT NULL, 
    [Name]          NVARCHAR(MAX) NOT NULL, 
    [Email]         NVARCHAR(MAX) NOT NULL, 
    [Settings] NVARCHAR(MAX) NULL , 
    CONSTRAINT [PK_User] PRIMARY KEY CLUSTERED ([Id]),
    CONSTRAINT [FK_User_User] FOREIGN KEY ([UpdatedBy]) REFERENCES [User]([Id]),
)
";
        public class SessionTokenEntity
        {
            public int FkUser { get; set; }
            public string SessionToken { get; set; }
            public DateTime TimeStamp { get; set; }
        }

        [TestMethod]
        public void TimeStamp_Insert_ShouldBeSuccessful( )
        {
            const string sqlInsert = "INSERT INTO [SessionToken] ([FkUser],[SessionToken],[TimeStamp]) VALUES(@id, @token, @timeStamp )";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlCreateTableSessionToken );
            connection.Execute( sqlInsert, new { id = 1, token = "test", timeStamp = DateTime.UtcNow } );

        }

        [TestMethod]
        public void TimeStamp_SelectBefore_ShouldBeSuccessful()
        {
            const string sqlInsert = "INSERT INTO [SessionToken] ([FkUser],[SessionToken],[TimeStamp]) VALUES(@id, @token, @timeStamp )";
            const string sqlSelect = "SELECT [FkUser],[SessionToken],[TimeStamp] FROM SessionToken WHERE [TimeStamp] < @timeStamp";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlCreateTableSessionToken );
            connection.Execute( sqlInsert, new { id = 1, token = "test", timeStamp = DateTime.UtcNow } );
            var tokens = connection.Query<SessionTokenEntity>( sqlSelect, new { timeStamp = DateTime.UtcNow.AddMinutes( 10 ) } );
            tokens.Should().NotBeEmpty();
        }

        [TestMethod]
        public void TimeStamp_SelectAfter_ShouldBeSuccessful()
        {
            const string sqlInsertUser = "INSERT INTO [User] ([Name],[Email]) VALUES(@name, @email )";
            const string sqlInsert = "INSERT INTO [SessionToken] ([FkUser],[SessionToken],[TimeStamp]) VALUES(@id, @token, @timeStamp )";
            const string sqlSelect = @"
SELECT 
    [SessionToken].[FkUser]
    , [SessionToken].[SessionToken]
    , [SessionToken].[TimeStamp]
FROM [User] 
INNER JOIN SessionToken ON SessionToken.FkUser = [User].Id AND SessionToken.TimeStamp <= @timeStamp 
WHERE IsDeleted = @isDeleted AND Email = @email";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlCreateTableUser );
            connection.Execute( sqlInsertUser, new { name = "test", email = "test@test.com"} );
            connection.Execute( SqlCreateTableSessionToken );
            connection.Execute( sqlInsert, new { id = 1, token = "test", timeStamp = DateTime.UtcNow } );
            var tokens = connection.Query<SessionTokenEntity>( sqlSelect, new { isDeleted = false, email = "test@test.com", timeStamp = DateTime.UtcNow.AddMinutes( 10 ) } );
            tokens.Count().Should().Be(1);
        }
    }
}
