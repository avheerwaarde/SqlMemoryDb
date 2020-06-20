using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

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
            var customers = connection.Query<CustomerViewDto>( "SELECT CompanyName, CustomerName from [Brazil Customers]" );
            customers.Count( ).Should( ).NotBe( 0 );
        }
    }
}
