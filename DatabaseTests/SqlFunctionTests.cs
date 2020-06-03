using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class SqlFunctionTests
    {
        [TestInitialize]
        public async Task InsertDb( )
        {
            await SqlScripts.InitDbAsync( );
        }

        [TestMethod]
        public async Task GetDate_Select_CurrentDateIsReturned( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, GetDate() as CurrentDate FROM application_feature WHERE Id = 1";
            await command.PrepareAsync( );
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                reader[ "Id" ].Should( ).Be( 1 );
                reader.GetDateTime(2).Should( ).BeCloseTo( DateTime.Now, TimeSpan.FromSeconds( 5 ) );
            }
        }

        [TestMethod]
        public async Task CountStar_Select_NumberOfRowsIsReturned( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Count(*) as [RowCount] FROM application";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "RowCount" ].Should( ).Be( 3 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 1 );
        }
    }
}
