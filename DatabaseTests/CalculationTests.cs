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
        [DataTestMethod]
        [DataRow("SELECT -75.0/6", -12.5)]
        //[DataRow("SELECT 1/2", 0)]
        //[DataRow("SELECT 1 + 1", 2.0)]
        //[DataRow("SELECT 1 + 2 + 3 + 4 + 99 + 704", 813)]
        //[DataRow("SELECT 1.5 + 1.5", 3.0)]
        //[DataRow("SELECT .25678 + .00096356", 0.25774356)]
        //[DataRow("SELECT 1.75 + -2.25", -0.50)]
        //[DataRow("SELECT 1 - 1", 0)]
        //[DataRow("SELECT 918 - 704", 214)]
        //[DataRow("SELECT 3.2 - 1.9", 1.3)]
        //[DataRow("SELECT 1.9 - 3.2", -1.3)]
        //[DataRow("SELECT 9 - 3 - 3", 3)]
        //[DataRow("SELECT .75 - .68", 0.07)]
        //[DataRow("SELECT 1 * 1", 1)]
        //[DataRow("SELECT 2 * -4", -8)]
        //[DataRow("SELECT 2 * 5 * 10", 100)]
        //[DataRow("SELECT 1.25 * 3", 3.75)]
        //[DataRow("SELECT .4 * .5", 0.20)]
        //[DataRow("SELECT 1.0/2.0", 0.5)]
        //[DataRow("SELECT 0/5", 0)]
        //[DataRow("SELECT 100/8", 12)]
        //[DataRow("SELECT 100.0/8", 12.5)]
        //[DataRow("SELECT .5/.1", 5)]
        //[DataRow("SELECT ((100 + 100) * .05)", 10)]
        //[DataRow("SELECT (10 - 5)/2", 2)]
        //[DataRow("SELECT (10.0 - 5.0)/2.0", 2.5)]
        //[DataRow("SELECT ((100 + 100) - (50 + 50))", 100)]
        public void Add_Fixed_CalculationIsReturned( string sql, double expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<decimal>( sql );
            value.Should( ).Be( Convert.ToDecimal(expected) );
        }


    }
}
