using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class SqlInsertTableTests
    {
        [TestInitialize]
        public async Task InsertDb( )
        {
            MemoryDbConnection.GetMemoryDatabase( ).Tables.Clear(  );

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
            var rows = MemoryDbConnection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            rows.Count.Should( ).Be( 1, "A new row should be added" );
            var row = rows.First( );
            row[ 0 ].Should( ).Be( 1 );
            row[ 1 ].Should( ).Be( "Name" );
            row[ 2 ].Should( ).Be( "User" );
            row[ 3 ].Should( ).Be( "DefName" );
        }

        [TestMethod]
        public async Task InsertApplication_TooManyColumns_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInsertTooManyColumnsException>( );
        }

        [TestMethod]
        public async Task InsertApplication_TooManyValues_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInsertTooManyValuesException>( );
        }

        [TestMethod]
        public async Task InsertApplication_MaxLengthExceeded_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefNameDefNameDefNameDefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlDataTruncatedException>( );
        }

        [TestMethod]
        public async Task InsertApplication_InvalidColumnName_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[Unknown]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInvalidColumnNameException>( );
        }

    }
}
