using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class StoredProcedureTests
    {
        private const string _SqlCreateUspSelectApplicationById = @"
CREATE PROCEDURE uspSelectApplicationById (@id AS INT)
AS
BEGIN
    SELECT  
	    Id
	    , Name
	    , [User]
	    , DefName
    FROM  application 
    WHERE Id = @id
END;";

        private const string _SqlCreateUspSelectApplications = @"
CREATE PROCEDURE uspSelectApplications
AS
BEGIN
    SELECT  
	    Id
	    , Name
	    , [User]
	    , DefName
    FROM  application 
END;";

        private const string _SqlAlterUspSelectApplications = @"
ALTER PROCEDURE uspSelectApplications
AS
BEGIN
    SELECT  
	    Id
	    , Name
	    , [User]
	    , DefName
    FROM  application 
END;";

        private const string _SqlDropUspSelectApplications = @"DROP PROCEDURE uspSelectApplications";

        [TestMethod]
        public void Create_uspSelectApplications_IsCreated( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            database.StoredProcedures.Count.Should( ).Be( 0 );
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateUspSelectApplications );
            database.StoredProcedures.Count.Should( ).Be( 1 );
        }

        [TestMethod]
        public void Create_uspSelectApplicationsExists_ThrowsException( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateUspSelectApplications );
            Func<int> act = ( ) => connection.Execute( _SqlCreateUspSelectApplications );
            act.Should( ).Throw<SqlObjectAlreadyExistsException>( );
        }

        [TestMethod]
        public void Alter_uspSelectApplicationsExists_IsCreated( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateUspSelectApplications );
            connection.Execute( _SqlAlterUspSelectApplications );
        }

        [TestMethod]
        public void Alter_uspSelectApplications_ThrowsException( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            using var connection = new MemoryDbConnection( );
            Func<int> act = ( ) => connection.Execute( _SqlAlterUspSelectApplications );
            act.Should( ).Throw<SqlInvalidObjectNameException>( );
        }

        [TestMethod]
        public void Drop_uspSelectApplicationsExists_IsCreated( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateUspSelectApplications );
            database.StoredProcedures.Count.Should( ).Be( 1 );
            connection.Execute( _SqlDropUspSelectApplications );
            database.StoredProcedures.Count.Should( ).Be( 0 );
        }

        [TestMethod]
        public void Drop_uspSelectApplications_ThrowsException( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            using var connection = new MemoryDbConnection( );
            Func<int> act = ( ) => connection.Execute( _SqlDropUspSelectApplications );
            act.Should( ).Throw<SqlDropProcedureException>( );
        }

        [TestMethod]
        public async Task Call_uspSelectApplications_ReturnsAllRows( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            await SqlScripts.InitDbAsync( );
            await using var connection = new MemoryDbConnection( );
            await connection.ExecuteAsync( _SqlCreateUspSelectApplications );
            var applications = await connection.QueryAsync<ApplicationDto>( "uspSelectApplications", commandType: CommandType.StoredProcedure );
            applications.Count( ).Should( ).Be( 3 );
        }

        [TestMethod]
        public async Task Call_uspSelectApplicationById_ReturnsSingleRows( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            await SqlScripts.InitDbAsync( );
            await using var connection = new MemoryDbConnection( );
            await connection.ExecuteAsync( _SqlCreateUspSelectApplicationById );
            var applications = await connection.QueryAsync<ApplicationDto>( "uspSelectApplicationById", new { id = 2 }, commandType: CommandType.StoredProcedure );
            applications.Count( ).Should( ).Be( 1 );
            applications.Single( ).Id.Should( ).Be( 2 );
        }

        [TestMethod]
        public async Task ExecuteLiteralParameter_uspSelectApplicationById_ReturnsSingleRows( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            await SqlScripts.InitDbAsync( );
            await using var connection = new MemoryDbConnection( );
            await connection.ExecuteAsync( _SqlCreateUspSelectApplicationById );
            var applications = await connection.QueryAsync<ApplicationDto>( "EXECUTE uspSelectApplicationById 2" );
            applications.Count( ).Should( ).Be( 1 );
            applications.Single( ).Id.Should( ).Be( 2 );
        }

        [TestMethod]
        public async Task ExecuteCommandParameter_uspSelectApplicationById_ReturnsSingleRows( )
        {
            var database = MemoryDbConnection.GetMemoryDatabase( );
            database.Clear(  );
            await SqlScripts.InitDbAsync( );
            await using var connection = new MemoryDbConnection( );
            await connection.ExecuteAsync( _SqlCreateUspSelectApplicationById );
            var applications = await connection.QueryAsync<ApplicationDto>( "EXECUTE uspSelectApplicationById @id", new { id = 2 } );
            applications.Count( ).Should( ).Be( 1 );
            applications.Single( ).Id.Should( ).Be( 2 );
        }
    }
}
