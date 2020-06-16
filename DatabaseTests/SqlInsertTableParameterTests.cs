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
    public class SqlInsertTableParameterTests
    {
        [TestInitialize]
        public async Task InitializeDb( )
        {
            MemoryDbConnection.GetMemoryDatabase( ).Clear(  );

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
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (@Name, @User, @DefName)";
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "Name", Value = "Name String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "User", Value = "User String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "DefName", Value = "DefName String"} );
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            var rows = MemoryDbConnection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            rows.Count.Should( ).Be( 1, "A new row should be added" );
            var row = rows.First( );
            row[ 0 ].Should( ).Be( 1 );
            row[ 1 ].Should( ).Be( "Name String" );
            row[ 2 ].Should( ).Be( "User String" );
            row[ 3 ].Should( ).Be( "DefName String" );
        }

        [TestMethod]
        public async Task InsertApplication_TooManyColumns_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (@Name, @User)";
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "Name", Value = "Name String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "User", Value = "User String"} );
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
            command.CommandText = "INSERT INTO application ([Name],[User]) VALUES (@Name, @User, @DefName)";
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "Name", Value = "Name String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "User", Value = "User String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "DefName", Value = "DefName String"} );
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
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (@Name, @User, @DefName)";
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "Name", Value = "Name String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "User", Value = "User String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "DefName", Value = "DefName String DefName String DefName String DefName String "} );
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
            command.CommandText = "INSERT INTO application ([Name],[User],[Unknown]) VALUES (@Name, @User, @DefName)";
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "Name", Value = "Name String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "User", Value = "User String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "DefName", Value = "DefName String"} );
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInvalidColumnNameException>( );
        }

        [TestMethod]
        public async Task InsertApplication_InvalidParameterName_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (@Name, @User, @NotToBeFound)";
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "Name", Value = "Name String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "User", Value = "User String"} );
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "DefName", Value = "DefName String"} );
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInvalidParameterNameException>( );
        }

    }
}
