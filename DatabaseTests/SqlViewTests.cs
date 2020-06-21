using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class SqlViewTests
    {
        private const string _SqlCreateViewBrazilianCustomers = @"
CREATE VIEW [Brazil Customers] AS
SELECT CompanyName, ContactName
FROM Customers
WHERE Country = 'Brazil';";

        private const string _SqlAlterViewBrazilianCustomers = @"
ALTER VIEW [Brazil Customers] AS
SELECT CompanyName, ContactName
FROM Customers
WHERE Country = 'Brazil';";

        private const string _SqlDropViewBrazilianCustomers = @"DROP VIEW [Brazil Customers]";

        

        [TestInitialize]
        public void InitializeDb( )
        {
            SqlScripts.InitNorthWindDatabase(  );
        }


        [TestMethod]
        public void View_Create_CreatedAndRead( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateViewBrazilianCustomers );
            var db = MemoryDbConnection.GetMemoryDatabase( );
            db.Views.Should( ).ContainKey( "dbo.Brazil Customers" );
            var customers = connection.Query<CustomerViewDto>( "SELECT CompanyName, ContactName from [Brazil Customers]" );
            customers.Count( ).Should( ).NotBe( 0 );
        }

        [TestMethod]
        public void View_AlterExisting_ViewAltered( )
        {
            var db = MemoryDbConnection.GetMemoryDatabase( );
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateViewBrazilianCustomers );
            db.Views.Should( ).ContainKey( "dbo.Brazil Customers" );
            connection.Execute( _SqlAlterViewBrazilianCustomers );
            db.Views.Should( ).ContainKey( "dbo.Brazil Customers" );
        }

        [TestMethod]
        public void View_DropExisting_ViewDeleted( )
        {
            var db = MemoryDbConnection.GetMemoryDatabase( );
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateViewBrazilianCustomers );
            db.Views.Should( ).ContainKey( "dbo.Brazil Customers" );
            connection.Execute( _SqlDropViewBrazilianCustomers );
            db.Views.Should( ).NotContainKey( "dbo.Brazil Customers" );
        }

        [TestMethod]
        public void View_AlterNew_ThrowsException( )
        {
            var db = MemoryDbConnection.GetMemoryDatabase( );
            using var connection = new MemoryDbConnection( );
            Func<int> act = ( ) => connection.Execute( _SqlAlterViewBrazilianCustomers );
            act.Should( ).Throw<SqlInvalidObjectNameException>( );
        }

        [TestMethod]
        public void View_DropNew_ThrowsException( )
        {
            var db = MemoryDbConnection.GetMemoryDatabase( );
            using var connection = new MemoryDbConnection( );
            Func<int> act = ( ) => connection.Execute( _SqlDropViewBrazilianCustomers );
            act.Should( ).Throw<SqlDropViewException>( );
        }

        [TestMethod]
        public void View_CreateTop10OrderBy_CreatedAndRead( )
        {
            const string sqlCreateViewBrazilianCustomers = @"
CREATE VIEW [Brazil Customers] AS
SELECT Top 10 CompanyName, ContactName
FROM Customers
WHERE Country = 'Brazil'
ORDER BY ContactName;";

            using var connection = new MemoryDbConnection( );
            connection.Execute( sqlCreateViewBrazilianCustomers );
            var db = MemoryDbConnection.GetMemoryDatabase( );
            db.Views.Should( ).ContainKey( "dbo.Brazil Customers" );
            var customers = connection.Query<CustomerViewDto>( "SELECT CompanyName, ContactName from [Brazil Customers]" );
            customers.Count( ).Should( ).BeLessOrEqualTo( 10 );
        }

        [TestMethod]
        public void View_CreateOrderBy_ThrowsException( )
        {
            const string sqlCreateViewBrazilianCustomers = @"
CREATE VIEW [Brazil Customers] AS
SELECT CompanyName, ContactName
FROM Customers
WHERE Country = 'Brazil'
ORDER BY ContactName;";

            using var connection = new MemoryDbConnection( );
            connection.Execute( sqlCreateViewBrazilianCustomers );
            var db = MemoryDbConnection.GetMemoryDatabase( );
            db.Views.Should( ).ContainKey( "dbo.Brazil Customers" );
            Func<IEnumerable<CustomerViewDto>> act = ( ) => connection.Query<CustomerViewDto>( "SELECT CompanyName, ContactName from [Brazil Customers]" );
            act.Should( ).Throw<SqlOrderByException>( );
        }
    }
}
