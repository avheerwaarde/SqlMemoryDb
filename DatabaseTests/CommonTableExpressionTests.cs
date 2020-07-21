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
    public class CommonTableExpressionTests
    {
        [TestInitialize]
        public void InitializeDb( )
        {
            SqlScripts.InitNorthWindDatabase(  );
        }

        [TestMethod]
        public void CommonTableExpression_Simple_Succeeds(  )
        {
            const string sql = @"
WITH [Brazil Customers] (CustomerID, CompanyName, ContactName)  
AS  
-- Define the CTE query.  
(  
SELECT CustomerID, CompanyName, ContactName
FROM Customers
WHERE Country = 'Brazil'  
)
SELECT CompanyName, ContactName from [Brazil Customers]";
            using var connection = new MemoryDbConnection( );
            var customers = connection.Query<CustomerViewDto>( sql );
            customers.Count( ).Should( ).BeGreaterThan( 1 );

        }

        [TestMethod]
        public void CommonTableExpression_Joined_Succeeds(  )
        {
            const string sql = @"
WITH [Brazil Customers] (CustomerID, CompanyName, ContactName)  
AS  
-- Define the CTE query.  
(  
SELECT CustomerID, CompanyName, ContactName
FROM Customers
WHERE Country = 'Brazil'  
),
[CustomerOrders] (CustomerID, NumberOfOrders)
AS
(
SELECT CustomerID, COUNT(1)
FROM Orders
GROUP BY CustomerID
)
SELECT CompanyName, ContactName, NumberOfOrders from [Brazil Customers]
JOIN [CustomerOrders] on [CustomerOrders].CustomerID = [Brazil Customers].CustomerID";

            using var connection = new MemoryDbConnection( );
            var customers = connection.Query<CustomerViewDto>( sql );
            customers.Count( ).Should( ).BeGreaterThan( 1 );

        }


    }
}
