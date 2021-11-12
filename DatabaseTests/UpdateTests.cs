using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using Dapper;
using DatabaseTests.Dto;

namespace DatabaseTests
{
    [TestClass]
    public class UpdateTests
    {
        [TestInitialize]
        public async Task InitializeDb( )
        {
            await SqlScripts.InitDbAsync( );
        }

        [TestMethod]
        public async Task UpdateAction_ChangeOrder_4RowsModified( )
        {
            const string sqlUpdate = @"
UPDATE application_action 
SET [Order] = 9
WHERE fk_application = 2
";

            await using var connection = new MemoryDbConnection( );
            int rowsAffected = await connection.ExecuteAsync( sqlUpdate );
            rowsAffected.Should( ).Be( 4 );
            var list = (await connection.QueryAsync<ApplicationActionDto>( SqlStatements.SqlSelectApplicationAction )).ToList(  );
            foreach ( var dto in list.Where( l => l.fk_application == 2 ) )
            {
                dto.Order.Should( ).Be( 9 );
            }
            foreach ( var dto in list.Where( l => l.fk_application != 2 ) )
            {
                dto.Order.Should( ).NotBe( 9 );
            }
        }

        [TestMethod]
        public async Task UpdateAction_NoRowsToUpdate_0RowsModified( )
        {
            const string sqlUpdate = @"
UPDATE application_action 
SET [Order] = 9
WHERE fk_application = 99
";

            await using var connection = new MemoryDbConnection( );
            int rowsAffected = await connection.ExecuteAsync( sqlUpdate );
            rowsAffected.Should( ).Be( 0 );
            var list = (await connection.QueryAsync<ApplicationActionDto>( SqlStatements.SqlSelectApplicationAction )).ToList(  );
            foreach ( var dto in list )
            {
                dto.Order.Should( ).NotBe( 9 );
            }
        }

        [TestMethod]
        public async Task UpdateAction_IncrementOrder_RowIsModified()
        {
            const string sqlUpdate = @"
UPDATE application_action 
SET [Order] = [Order] + 1
WHERE Id = 1
";

            await using var connection = new MemoryDbConnection();
            for ( int count = 1; count < 5; count++ )
            {
                var originalAction = (await connection.QuerySingleOrDefaultAsync<ApplicationActionDto>(SqlStatements.SqlSelectApplicationAction + " WHERE Id = 1"));
                int rowsAffected = await connection.ExecuteAsync(sqlUpdate);
                rowsAffected.Should().Be(1);
                var newAction = (await connection.QuerySingleOrDefaultAsync<ApplicationActionDto>(SqlStatements.SqlSelectApplicationAction + " WHERE Id = 1"));
                newAction.Order.Should().Be( originalAction.Order + 1);
            }
        }

    }
}
