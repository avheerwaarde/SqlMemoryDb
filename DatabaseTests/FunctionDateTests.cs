using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class FunctionDateTests
    {
        [TestMethod]
        public async Task GetDate_Select_CurrentDateIsReturned( )
        {
            await SqlScripts.InitDbAsync( );
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Id, GetDate() as CurrentDate FROM application_feature WHERE Id = 1";
            await command.PrepareAsync( );
            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                reader[ "Id" ].Should( ).Be( 1 );
                reader.GetDateTime(1).Should( ).BeCloseTo( DateTime.Now, TimeSpan.FromSeconds( 5 ) );
            }
        }

        [DataTestMethod]
        [DataRow("SELECT DATEADD(year, 1, '2014/04/28');", "2015-04-28")]
        [DataRow("SELECT DATEADD(yyyy, 1, '2014/04/28');", "2015-04-28")]
        [DataRow("SELECT DATEADD(yy, 1, '2014/04/28');", "2015-04-28")]
        [DataRow("SELECT DATEADD(year, -1, '2014/04/28');", "2013-04-28")]
        [DataRow("SELECT DATEADD(month, 1, '2014/04/28');", "2014-05-28")]
        [DataRow("SELECT DATEADD(month, -1, '2014/04/28');", "2014-03-28")]
        [DataRow("SELECT DATEADD(day, 1, '2014/04/28');", "2014-04-29")]
        [DataRow("SELECT DATEADD(day, -1, '2014/04/28');", "2014-04-27")]
        [DataRow("SELECT DATEADD(year,1, '2020-02-29'); ", "2021-02-28")]
        public void DateAdd_Fixed_DateIsReturned( string sql, string expectedAsString )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<DateTime>( sql );
            var expected = DateTime.Parse( expectedAsString );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT DATEDIFF(year, '2012/04/28', '2014/04/28');", 2)]
        [DataRow("SELECT DATEDIFF(yyyy, '2012/04/28', '2014/04/28');", 2)]
        [DataRow("SELECT DATEDIFF(yy, '2012/04/28', '2014/04/28');", 2)]
        [DataRow("SELECT DATEDIFF(month, '2014/01/01', '2014/04/28');", 3)]
        [DataRow("SELECT DATEDIFF(day, '2014/01/01', '2014/04/28');", 117)]
        [DataRow("SELECT DATEDIFF(hour, '2014/04/28 08:00', '2014/04/28 10:45');", 2)]
        [DataRow("SELECT DATEDIFF(minute, '2014/04/28 08:00', '2014/04/28 10:45');", 165)]
        public void DateDiff_Fixed_DateIsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT DATENAME(year, '2014/04/28');", "2014")]
        [DataRow("SELECT DATENAME(yyyy, '2014/04/28');", "2014")]
        [DataRow("SELECT DATENAME(yy, '2014/04/28');", "2014")]
        [DataRow("SELECT DATENAME(month, '2014/04/28');", "April")]
        [DataRow("SELECT DATENAME(day, '2014/04/28');", "28")]
        [DataRow("SELECT DATENAME(quarter, '2014/04/28');", "2")]
        [DataRow("SELECT DATENAME(hour, '2014/04/28 09:49');", "9")]
        [DataRow("SELECT DATENAME(minute, '2014/04/28 09:49');", "49")]
        [DataRow("SELECT DATENAME(second, '2014/04/28 09:49:12');", "12")]
        [DataRow("SELECT DATENAME(millisecond, '2014/04/28 09:49:12.726');", "726")]
        public void DateName_Fixed_StringIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT DATEPART(year, '2014/04/28');", 2014)]
        [DataRow("SELECT DATEPART(yyyy, '2014/04/28');", 2014)]
        [DataRow("SELECT DATEPART(yy, '2014/04/28');",2014 )]
        [DataRow("SELECT DATEPART(month, '2014/04/28');", 4)]
        [DataRow("SELECT DATEPART(day, '2014/04/28');", 28)]
        [DataRow("SELECT DATEPART(quarter, '2014/04/28');", 2)]
        [DataRow("SELECT DATEPART(hour, '2014/04/28 09:49');", 9)]
        [DataRow("SELECT DATEPART(minute, '2014/04/28 09:49');", 49)]
        [DataRow("SELECT DATEPART(second, '2014/04/28 09:49:12');", 12 )]
        [DataRow("SELECT DATEPART(millisecond, '2014/04/28 09:49:12.726');", 726 )]
        public void DatePart_Fixed_ValueIsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( sql );
            scalar.Should( ).Be( expected );
        }


        [DataTestMethod]
        [DataRow("SELECT DAY('2014/04/28');", 28)]
        [DataRow("SELECT DAY('2014/03/31 10:05');", 31)]
        [DataRow("SELECT DAY('2014/04/01 10:05:18.621');", 1)]
        public void Day_Fixed_ValueIsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( sql );
            scalar.Should( ).Be( expected );
        }
        
        [DataTestMethod]
        [DataRow( "SELECT SYSDATETIME();  ", false )]
        [DataRow( "SELECT SYSDATETIMEOFFSET();", false )]
        [DataRow( "SELECT SYSUTCDATETIME();  ", true )]
        [DataRow( "SELECT CURRENT_TIMESTAMP;  ", false )]
        [DataRow( "SELECT GETDATE();  ", false )]
        [DataRow( "SELECT GETUTCDATE();  ", true )]
        public void GetDates_Fixed_CurrentDateIsReturned( string sql, bool isUtc )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<DateTime?>( sql );
            if (  isUtc )
            {
                scalar.Should( ).BeCloseTo( DateTime.UtcNow );
            }
            else
            {
                scalar.Should( ).BeCloseTo( DateTime.Now );
            }
        }

        [DataTestMethod]
        [DataRow("SELECT MONTH('2014/04/28');", 4)]
        [DataRow("SELECT MONTH('2014/03/31 10:05');", 3)]
        [DataRow("SELECT MONTH('2014/12/01 10:05:18.621');", 12)]
        public void Month_Fixed_ValueIsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT YEAR('2014/04/28');", 2014)]
        [DataRow("SELECT YEAR('2013/03/31 10:05');", 2013)]
        [DataRow("SELECT YEAR('2015/12/01 10:05:18.621');", 2015)]
        public void Year_Fixed_ValueIsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( sql );
            scalar.Should( ).Be( expected );
        }


    }
}
