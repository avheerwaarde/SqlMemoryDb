using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class CalculationTests
    {
        [TestMethod]
        public void Add_Fixed_CalculationIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<int>( "SELECT 1+2" );
            value.Should( ).Be( 3 );
        }


    }
}
