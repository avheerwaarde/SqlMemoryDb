using System;
using System.Collections.Generic;
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
WITH [Brazil Customers] (CompanyName, ContactName)  
AS  
-- Define the CTE query.  
(  
SELECT CompanyName, ContactName
FROM Customers
WHERE Country = 'Brazil'  
)
SELECT CompanyName, ContactName from [Brazil Customers]";
            using var connection = new MemoryDbConnection( );
            var customers = connection.Query<CustomerViewDto>( sql );
        }
    }
}
