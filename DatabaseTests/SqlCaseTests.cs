using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class SqlCaseTests
    {
        [TestInitialize]
        public void InitDb( )
        {
            SqlScripts.InitNorthWindDatabase(  );
        }

        [TestMethod]
        public void Case_Simple_Succeeds( )
        {
            const string sql = @"SELECT OrderID, Quantity,
CASE
    WHEN Quantity > 30 THEN 'The quantity is greater than 30'
    WHEN Quantity = 30 THEN 'The quantity is 30'
    ELSE 'The quantity is under 30'
END AS QuantityText
FROM [Order Details];";

            using var connection = new MemoryDbConnection( );
            var rows = connection.Query<OrderDetailsWithCaseDto>( sql );
            foreach ( var row in rows )
            {
                if ( row.Quantity > 30 )
                {
                    row.QuantityText.Should( ).Be( "The quantity is greater than 30" );
                }
                else if ( row.Quantity == 30 )
                {
                    row.QuantityText.Should( ).Be( "The quantity is 30" );
                }
                else
                {
                    row.QuantityText.Should( ).Be( "The quantity is under 30" );
                }
            }
        }

        [DataTestMethod]
        [DataRow(20)]
        [DataRow(30)]
        [DataRow(40)]
        public void Case_SimpleParameter_Succeeds( int cutoffPoint )
        {
            string sql = $@"SELECT OrderID, Quantity,
CASE
    WHEN Quantity > @CutoffPoint THEN 'The quantity is greater than {cutoffPoint}'
    WHEN Quantity = @CutoffPoint THEN 'The quantity is {cutoffPoint}'
    ELSE 'The quantity is under {cutoffPoint}'
END AS QuantityText
FROM [Order Details];";

            using var connection = new MemoryDbConnection( );
            var rows = connection.Query<OrderDetailsWithCaseDto>( sql, new { CutoffPoint = (short)cutoffPoint } );
            foreach ( var row in rows )
            {
                if ( row.Quantity > cutoffPoint )
                {
                    row.QuantityText.Should( ).Be( $"The quantity is greater than {cutoffPoint}" );
                }
                else if ( row.Quantity == cutoffPoint )
                {
                    row.QuantityText.Should( ).Be( $"The quantity is {cutoffPoint}" );
                }
                else
                {
                    row.QuantityText.Should( ).Be( $"The quantity is under {cutoffPoint}" );
                }
            }
        }

        [TestMethod]
        public void Case_OrderBy_Succeeds( )
        {
            const string sql = @"
SELECT ContactName, City, Country
FROM Customers
ORDER BY
(CASE
    WHEN City IS NULL THEN Country
    ELSE City
END);";

            using var connection = new MemoryDbConnection( );
            var rows = connection.Query<CustomerWithCaseDto>( sql );
        }


    }
}
