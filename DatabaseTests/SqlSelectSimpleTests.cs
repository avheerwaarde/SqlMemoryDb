using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class SqlSelectSimpleTests
    {
        [TestInitialize]
        public async Task InitializeDb( )
        {
            await SqlScripts.InitDbAsync( );
        }

        [TestMethod]
        public async Task SelectApplication_CorrectSql_RowsRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, [Name], [User], DefName AS DefaultName FROM application";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                reader[ "Id" ].Should( ).Be( recordsRead + 1 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "User" ].Should( ).Be( "User String" );
                reader[ "DefaultName" ].Should( ).Be( "DefName String" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 3 );
        }

        [TestMethod]
        public async Task SelectApplication_CorrectSqlSingleRow_RowRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, [Name], [User], DefName AS DefaultName FROM application WHERE Id = 2";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                reader[ "Id" ].Should( ).Be( 2 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "User" ].Should( ).Be( "User String" );
                reader[ "DefaultName" ].Should( ).Be( "DefName String" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task SelectApplication_CorrectSqlSingleRow2_RowRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, [Name], [User], DefName AS DefaultName FROM application WHERE 2 = Id";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                reader[ "Id" ].Should( ).Be( 2 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "User" ].Should( ).Be( "User String" );
                reader[ "DefaultName" ].Should( ).Be( "DefName String" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task SelectApplication_CorrectSqlSingleRowParameter_RowRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, [Name], [User], DefName AS DefaultName FROM application WHERE Id = @Id";
            command.Parameters.Add( new MemoryDbParameter( ) {ParameterName = "Id", Value = 2} );
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                reader[ "Id" ].Should( ).Be( 2 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "User" ].Should( ).Be( "User String" );
                reader[ "DefaultName" ].Should( ).Be( "DefName String" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 1 );
        }


        [TestMethod]
        public async Task SelectApplication_WithLiterals_RowRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, N'Literal string' AS Name, 42 AS Magic FROM application WHERE Id = 2";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                reader[ "Id" ].Should( ).Be( 2 );
                reader[ "Name" ].Should( ).Be( "Literal string" );
                reader[ "Magic" ].Should( ).Be( 42 );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task SelectApplicationAction_Top4_RowRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT TOP 4 Id FROM application_action";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                recordsRead++;
            }

            recordsRead.Should( ).Be( 4 );
        }

        [TestMethod]
        public async Task SelectApplicationAction_WhereOr_RowsRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, [Name], Action, [Order], fk_application FROM application_action WHERE fk_application = 2 OR [Order] = 1";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                recordsRead++;
            }

            recordsRead.Should( ).Be( 6 );
        }

        [TestMethod]
        public async Task SelectApplicationAction_WhereAnd_RowsRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, [Name], Action, [Order], fk_application FROM application_action WHERE fk_application = 2 AND [Order] = 1";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                recordsRead++;
            }

            recordsRead.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task SelectApplicationAction_WhereAndOr_RowsRead( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, [Name], Action, [Order], fk_application FROM application_action WHERE (fk_application = 2 AND [Order] = 1) Or [Order] = 3";
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                recordsRead++;
            }

            recordsRead.Should( ).Be( 4 );
        }

        [TestMethod]
        public void SelectApplication_WhereIn_RowsRead( )
        {
            const string sql = @"
SELECT  
	Id, Name, [User], DefName
FROM  application 
WHERE Id IN (1,2)
";
            using var connection = new MemoryDbConnection( );
            var applications = connection.Query<ApplicationDto>( sql );
            applications.Count( ).Should( ).Be( 2 );
        }

        [TestMethod]
        public void SelectApplication_WhereNotIn_RowsRead( )
        {
            const string sql = @"
SELECT  
	Id, Name, [User], DefName
FROM  application 
WHERE Id Not IN (1,2)
";
            using var connection = new MemoryDbConnection( );
            var applications = connection.Query<ApplicationDto>( sql );
            applications.Count( ).Should( ).Be( 1 );
        }


        [TestMethod]
        public void SelectApplication_WhereInSelect_RowsRead( )
        {
            const string sql = @"
SELECT  
	Id, Name, [User], DefName
FROM  application 
WHERE Id IN (SELECT fk_application from application_action WHERE fk_application = 1 OR fk_application = 2)
";
            using var connection = new MemoryDbConnection( );
            var applications = connection.Query<ApplicationDto>( sql );
            applications.Count( ).Should( ).Be( 2 );
        }

    }
}
