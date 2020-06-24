using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class DeleteTests
    {
        [TestInitialize]
        public async Task InitializeDb( )
        {
            await SqlScripts.InitDbAsync( );
        }

        [TestMethod]
        public void Delete_AllRows_AllRowsAreDeleted( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( "DELETE FROM application" );
            var applications = connection.Query<ApplicationDto>( SqlStatements.SqlSelectApplication );
            applications.Count( ).Should( ).Be( 0 );
        }

        [TestMethod]
        public void Delete_SingleRow_RowIsDeleted( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( "DELETE FROM application WHERE Id = 1" );
            var applications = connection.Query<ApplicationDto>( SqlStatements.SqlSelectApplication );
            applications.Count( ).Should( ).Be( 2 );
        }

        [TestMethod]
        public void Delete_Top2_RowsAreDeleted( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( "DELETE TOP(2) FROM application" );
            var applications = connection.Query<ApplicationDto>( SqlStatements.SqlSelectApplication );
            applications.Count( ).Should( ).Be( 1 );
        }

        [TestMethod]
        public void Delete_Join_SingleRowIsDeleted( )
        {
            const string sql = @"
DELETE a
FROM application AS a
INNER JOIN application_action ON application.Id = application_action.fk_application
WHERE application_action.fk_application = 1";

            using var connection = new MemoryDbConnection( );
            connection.Execute( sql );
            var applications = connection.Query<ApplicationDto>( SqlStatements.SqlSelectApplication );
            applications.Count( ).Should( ).Be( 2 );
        }

    }
}
