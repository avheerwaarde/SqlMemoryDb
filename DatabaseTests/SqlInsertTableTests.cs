using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class SqlInsertTableTests
    {
        [TestInitialize]
        public async Task InsertDb( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication + "\n" 
                                + SqlStatements.SqlCreateTableApplicationFeature + "\n" 
                                + SqlStatements.SqlCreateTableApplicationAction ;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
        }
        
        [TestMethod]
        public async Task InsertApplication_CorrectSql_RowIsAdded( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
        }
    }
}
