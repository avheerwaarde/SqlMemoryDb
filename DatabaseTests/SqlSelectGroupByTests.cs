using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class SqlSelectGroupByTests
    {
        [TestInitialize]
        public async Task InitializeDb( )
        {
            await SqlScripts.InitDbAsync( );
        }

        [TestMethod]
        public async Task GroupBy_ApplicationWithCount_GroupedResult( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT fk_application AS ApplicationId, count(*) AS GroupSize FROM application_action GROUP BY fk_application";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "GroupSize" ].Should( ).Be( 4 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 3 );
        }

        [TestMethod]
        public async Task GroupBy_ColumnNotInGroupBy_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, count(*) AS GroupSize FROM application_action GROUP BY fk_application";
            await command.PrepareAsync( );

            Func<Task> act = async () => { await command.ExecuteReaderAsync( ); };
            await act.Should( ).ThrowAsync<SqlNoAggregateFieldException>( );

        }

        [TestMethod]
        public async Task GroupByHaving_ApplicationWithCount_GroupedResult( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT fk_application AS ApplicationId, count(*) AS GroupSize FROM application_action GROUP BY fk_application HAVING count(*) > 2";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "GroupSize" ].Should( ).Be( 4 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 3 );
        }

        [TestMethod]
        public async Task GroupByHaving_ApplicationWithCount_NoResult( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT fk_application AS ApplicationId, count(*) AS GroupSize FROM application_action GROUP BY fk_application HAVING count(*) < 2";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "GroupSize" ].Should( ).Be( 4 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 0 );
        }

        [TestMethod]
        public async Task HavingNoGroupBy_ValidPremise_ReturnsResult( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT count(*) AS GroupSize FROM application HAVING count(*) > 2";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "GroupSize" ].Should( ).Be( 3 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task HavingNoGroupBy_FailingPremise_NoResult( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT count(*) AS GroupSize FROM application HAVING count(*) < 2";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "GroupSize" ].Should( ).Be( 3 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 0 );
        }


    }
}
