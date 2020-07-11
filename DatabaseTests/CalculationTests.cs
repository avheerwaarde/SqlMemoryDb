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
    public class CalculationTests
    {
        [DataTestMethod]
        [DataRow("SELECT 2 * -4", -8)]
        [DataRow("SELECT ((100 + 100) * .05)", 10)]
        [DataRow("SELECT (10 - 5)/2", 2)]
        [DataRow("SELECT (10.0 - 5.0)/2.0", 2.5)]
        [DataRow("SELECT -75.0/6", -12.5)]
        [DataRow("SELECT 1/2", 0)]
        [DataRow("SELECT 1 + 1", 2.0)]
        [DataRow("SELECT 1 + 2 + 3 + 4 + 99 + 704", 813)]
        [DataRow("SELECT 1.5 + 1.5", 3.0)]
        [DataRow("SELECT .25678 + .00096356", 0.25774356)]
        [DataRow("SELECT 1.75 + -2.25", -0.50)]
        [DataRow("SELECT 1 - 1", 0)]
        [DataRow("SELECT 918 - 704", 214)]
        [DataRow("SELECT 3.2 - 1.9", 1.3)]
        [DataRow("SELECT 1.9 - 3.2", -1.3)]
        [DataRow("SELECT 9 - 3 - 3", 3)]
        [DataRow("SELECT .75 - .68", 0.07)]
        [DataRow("SELECT 1 * 1", 1)]
        [DataRow("SELECT 2 * 5 * 10", 100)]
        [DataRow("SELECT 1.25 * 3", 3.75)]
        [DataRow("SELECT .4 * .5", 0.20)]
        [DataRow("SELECT 1.0/2.0", 0.5)]
        [DataRow("SELECT 0/5", 0)]
        [DataRow("SELECT 100/8", 12)]
        [DataRow("SELECT 100.0/8", 12.5)]
        [DataRow("SELECT .5/.1", 5)]
        [DataRow("SELECT ((100 + 100) - (50 + 50))", 100)]
        public void CalculateNumbers_Fixed_CalculationIsReturned( string sql, double expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<decimal>( sql );
            value.Should( ).Be( Convert.ToDecimal(expected) );
        }

        [DataTestMethod]
        [DataRow("SELECT 'Hello'+' world'", "Hello world")]
        [DataRow("SELECT 'Hello'+' world'", "Hello world")]
        [DataRow("SELECT 'Hello'+' my' + ' world'", "Hello my world")]
        [DataRow("SELECT 'Hello'+ 4 ", "Hello4")]
        public void CalculateStrings_Fixed_ConcatenationIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<string>( sql );
            value.Should( ).Be( expected );
        }


        [DataTestMethod]
        [DataRow("SELECT @i1 + @i2 + @i3 + @d1 + @d2 + @d3", 232.575 )]
        [DataRow("SELECT @d2 + -@d3", 4.925)]
        [DataRow("SELECT @i2 - @i3", 25)]
        [DataRow("SELECT @d2 - @d3", 4.925)]
        [DataRow("SELECT @i2 * @i3", 3750)]
        [DataRow("SELECT @d2 * @d3", 3.1625)]
        [DataRow("SELECT @i2 / @i3", 1)]
        [DataRow("SELECT ((@i1 + @i2) * @d2)", 962.50)]
        [DataRow("SELECT ((@i1 + @i2) - (@d1 + @d2))", 168.00)]
        public void CalculateNumbers_Parameters_CalculationIsReturned( string sql, double expected )
        {
            const string sqlDeclarations = @"
-- Variable declaration
DECLARE @i1 int
DECLARE @i2 int
DECLARE @i3 int
DECLARE @d1 decimal(10,2)
DECLARE @d2 decimal(10,2)
DECLARE @d3 decimal(10,2)
-- Initialize variables
SET @i1 = 100
SET @i2 = 75
SET @i3 = 50
SET @d1 = 1.5
SET @d2 = 5.5
SET @d3 = .575
";

            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<decimal>( sqlDeclarations + sql );
            value.Should( ).Be( Convert.ToDecimal(expected) );
        }

        [TestMethod]
        public void CalculateNumbers_Columns_CalculationIsReturned( )
        {
            const string sql = @"
-- Sample Table
CREATE TABLE dbo.CalculationExample(
ProductID int NOT NULL,
Cost decimal(10,2) NOT NULL)
GO
-- Populate Table
INSERT INTO dbo.CalculationExample (ProductID, Cost)
SELECT 1, 100.00
UNION
SELECT 2, 50.00
UNION
SELECT 3, 25.00
GO

-- Declare Variables
DECLARE @MarginPercent decimal(10, 2)
DECLARE @TaxPercent decimal(10, 2)

-- Initialize Variables
SET @MarginPercent = .20
SET @TaxPercent = .05

-- Calculate Values
SELECT ProductID,
Cost,
Cost * @MarginPercent AS 'Margin',
Cost * @TaxPercent AS 'Tax',
Cost + (Cost * @MarginPercent) + (Cost * @TaxPercent) AS 'FinalCost'
FROM dbo.CalculationExample";

            using var connection = new MemoryDbConnection( );
            var dtoList = connection.Query<CalculationDto>( sql );
        }



    }
}
