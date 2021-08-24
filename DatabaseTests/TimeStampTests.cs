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

        [TestMethod]
        public void TimeStamp_Insert_ShouldBeSuccessful( )
        {
            const string sqlInsert = "INSERT INTO [SessionToken] ([FkUser],[SessionToken],[TimeStamp]) VALUES(@id, @token, @timeStamp )";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlCreateTableSessionToken );
            connection.Execute( sqlInsert, new { id = 1, token = "test", timeStamp = DateTime.UtcNow } );

        }
    }
}
