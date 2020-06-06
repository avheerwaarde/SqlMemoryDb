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
    public class ExecuteNonQueryTests
    {
        [TestMethod]
        public async Task OpenConnection_CreateTable_Ok( )
        {
            MemoryDbConnection.GetMemoryDatabase( ).Tables.Clear(  );
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            var db = MemoryDbConnection.GetMemoryDatabase( );
            db.Tables.Count.Should( ).Be( 1 );
        }
    }
}
