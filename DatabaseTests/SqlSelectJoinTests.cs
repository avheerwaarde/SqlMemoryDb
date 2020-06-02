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
    public class SqlSelectJoinTests
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

            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name String', N'User String', N'DefName String')";
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            await command.ExecuteNonQueryAsync( );
            await command.ExecuteNonQueryAsync( );

            for ( int applicationId = 1; applicationId <= 3; applicationId++ )
            {
                for ( int index = 1; index <= 4; index++ )
                {
                    command.CommandText = $"INSERT INTO application_action ([Name],[Action],[Order],[fk_application]) VALUES (N'Action {applicationId}-{index}', N'Do Something {applicationId}-{index}', {index}, {applicationId})";
                    await command.PrepareAsync( );
                    await command.ExecuteNonQueryAsync( );
                }
            }
        }

        [TestMethod]
        public async Task SelectApplicationAction_InnerJoinPlain_AllProductRowsRead( )
        {
            const string sqlSelect = @"
SELECT  
	application.Id
	, application.Name
	, application.[User]
	, DefName
	, application_action.Id AS ActionId
	, application_action.Name AS ActionName
	, application_action.Action
	, application_action.fk_application
	, [Order]
FROM  application 
INNER JOIN application_action ON application.Id = application_action.fk_application";

            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = sqlSelect;
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                ((int)reader[ "Id" ]).Should( ).BeInRange( 1, 3 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "ActionName" ].ToString().Should( ).StartWith( "Action" );
                reader[ "Action" ].ToString().Should( ).StartWith( "Do Something" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 12 );
        }

        [TestMethod]
        public async Task SelectApplicationAction_InnerJoinAliasFrom_AllProductRowsRead( )
        {
            const string sqlSelect = @"
SELECT  
	app.Id
	, app.Name
	, app.[User]
	, DefName
	, application_action.Id AS ActionId
	, application_action.Name AS ActionName
	, application_action.Action
	, application_action.fk_application
	, [Order]
FROM  application app
INNER JOIN application_action ON app.Id = application_action.fk_application";

            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = sqlSelect;
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                ((int)reader[ "Id" ]).Should( ).BeInRange( 1, 3 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "ActionName" ].ToString().Should( ).StartWith( "Action" );
                reader[ "Action" ].ToString().Should( ).StartWith( "Do Something" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 12 );
        }

        [TestMethod]
        public async Task SelectApplicationAction_InnerJoinAliasJoin_AllProductRowsRead( )
        {
            const string sqlSelect = @"
SELECT  
	application.Id
	, application.Name
	, application.[User]
	, DefName
	, action.Id AS ActionId
	, action.Name AS ActionName
	, action.Action
	, action.fk_application
	, [Order]
FROM  application 
INNER JOIN application_action action ON application.Id = action.fk_application";

            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = sqlSelect;
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                ((int)reader[ "Id" ]).Should( ).BeInRange( 1, 3 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "ActionName" ].ToString().Should( ).StartWith( "Action" );
                reader[ "Action" ].ToString().Should( ).StartWith( "Do Something" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 12 );
        }

        [TestMethod]
        public async Task SelectApplicationAction_InnerJoinAliasFromJoin_AllProductRowsRead( )
        {
            const string sqlSelect = @"
SELECT  
	app.Id
	, app.Name
	, app.[User]
	, DefName
	, action.Id AS ActionId
	, action.Name AS ActionName
	, action.Action
	, action.fk_application
	, [Order]
FROM  application app
INNER JOIN application_action action ON app.Id = action.fk_application";

            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = sqlSelect;
            await command.PrepareAsync( );

            var recordsRead = 0;
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                ((int)reader[ "Id" ]).Should( ).BeInRange( 1, 3 );
                reader[ "Name" ].Should( ).Be( "Name String" );
                reader[ "ActionName" ].ToString().Should( ).StartWith( "Action" );
                reader[ "Action" ].ToString().Should( ).StartWith( "Do Something" );
                recordsRead++;
            }

            recordsRead.Should( ).Be( 12 );
        }
    }
}
