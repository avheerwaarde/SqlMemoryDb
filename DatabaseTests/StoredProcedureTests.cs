using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class StoredProcedureTests
    {
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

    }
}
