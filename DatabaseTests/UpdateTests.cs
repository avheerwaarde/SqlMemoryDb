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
    public class UpdateTests
    {
        [TestInitialize]
        public async Task InsertDb( )
        {
            await SqlScripts.InitDbAsync( );
        }

        [TestMethod]
        public async Task UpdateAction_ChangeOrder_4RowsModified( )
        {
            const string sql = @"
UPDATE application_action 
SET [Order] = 9
WHERE fk_application = 2
";
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = sql;
            await command.PrepareAsync( );
            var rowsAffected = await command.ExecuteNonQueryAsync( );
            rowsAffected.Should( ).Be( 4 );
        }
    }
}
