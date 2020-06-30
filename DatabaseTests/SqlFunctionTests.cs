using System;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class SqlFunctionTests
    {
        [TestInitialize]
        public async Task InitializeDb( )
        {
            await SqlScripts.InitDbAsync( );
        }

        [TestMethod]
        public async Task GetDate_Select_CurrentDateIsReturned( )
        {
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

        [TestMethod]
        public async Task CountStar_Select_NumberOfRowsIsReturned( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Count(*) as [RowCount] FROM application";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "RowCount" ].Should( ).Be( 3 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task Count1_Select_NumberOfRowsIsReturned( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Count(1) as [RowCount] FROM application";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "RowCount" ].Should( ).Be( 3 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task Max_SelectOrder_MaxOrderIsReturned( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT max([Order]) as [MaxOrder] FROM application_action";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "MaxOrder" ].Should( ).Be( 4 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task Min_SelectOrder_MinOrderIsReturned( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT MIN([Order]) as [MinOrder] FROM application_action";
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            var rowCounter = 0;
            while ( await reader.ReadAsync() )
            {
                reader[ "MinOrder" ].Should( ).Be( 1 );
                rowCounter++;
            }

            rowCounter.Should( ).Be( 1 );
        }

        [TestMethod]
        public async Task Invalid_UnknownFunction_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "SELECT Invalid([Order]) as [MinOrder] FROM application_action";
            await command.PrepareAsync( );

            Func<Task> act = async () => { await command.ExecuteReaderAsync( ); };
            await act.Should( ).ThrowAsync<SqlFunctionNotSupportedException>( );
        }

        [TestMethod]
        public void Sum_SelectOrder_SumOrderIsReturned( )
        {
            using var connection = new MemoryDbConnection( );
            var sum = connection.ExecuteScalar<int>( "SELECT Sum([Order]) FROM application_action" );
            sum.Should( ).Be( 30 );
        }

        [TestMethod]
        public void Avg_SelectOrder_AverageOrderIsReturned( )
        {
            using var connection = new MemoryDbConnection( );
            var sum = connection.ExecuteScalar<double>( "SELECT Avg([Order]) FROM application_action" );
            sum.Should( ).Be( 2.5 );
        }

        [TestMethod]
        public void Ceiling_Fixed_CeilingIsReturned( )
        {
            using var connection = new MemoryDbConnection( );
            var ceiling = connection.ExecuteScalar<int>( "SELECT Ceiling(2.5)" );
            ceiling.Should( ).Be( 3 );
        }

        [TestMethod]
        public void Ceiling_Field_CeilingIsReturned( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( "DELETE FROM application_feature" );
            connection.Execute( "INSERT INTO application_feature ([float]) VALUES (2.5)" );
            var ceiling = connection.ExecuteScalar<int>( "SELECT Ceiling([float]) from application_feature" );
            ceiling.Should( ).Be( 3 );
        }

        [TestMethod]
        public void Floor_Fixed_FloorIsReturned( )
        {
            using var connection = new MemoryDbConnection( );
            var floor = connection.ExecuteScalar<int>( "SELECT Floor(2.5)" );
            floor.Should( ).Be( 2 );
        }

        [TestMethod]
        public void Floor_Field_FloorIsReturned( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( "DELETE FROM application_feature" );
            connection.Execute( "INSERT INTO application_feature ([float]) VALUES (2.5)" );
            var floor = connection.ExecuteScalar<int>( "SELECT Floor([float]) from application_feature" );
            floor.Should( ).Be( 2 );
        }

        [DataTestMethod]
        [DataRow( "125.315, 2", 125.32 )]
        [DataRow( "125.315, 2, 0", 125.32 )]
        [DataRow( "125.315, 2, 1", 125.31 )]
        [DataRow( "125.315, 1", 125.3 )]
        [DataRow( "125.315, 0", 125 )]
        [DataRow( "125.315, -1", 130 )]
        [DataRow( "125.315, -2", 100 )]
        public void Round_Fixed_RoundedValueIsReturned( string arguments, double expectedResult )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<double>( $"SELECT ROUND({arguments});" );
            scalar.Should( ).Be( expectedResult );
        }

        [DataTestMethod]
        [DataRow( "-125.315", 125.315 )]
        [DataRow( "125.315", 125.315 )]
        [DataRow( "100", 100 )]
        [DataRow( "-100", 100 )]
        public void Abs_Fixed_AbsoluteValueIsReturned( string arguments, double expectedResult )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<double>( $"SELECT ABS({arguments});" );
            scalar.Should( ).Be( expectedResult );
        }

        [DataTestMethod]
        [DataRow( "-125.315", -1 )]
        [DataRow( "0.0", 0 )]
        [DataRow( "125.315", 1 )]
        [DataRow( "100", 1)]
        [DataRow( "0", 0 )]
        [DataRow( "-100", -1 )]
        public void Sign_Fixed_SignOfValueIsReturned( string arguments, double expectedResult )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<double>( $"SELECT Sign({arguments});" );
            scalar.Should( ).Be( expectedResult );
        }

        [DataTestMethod]
        [DataRow( "Rand()" )]
        [DataRow( "Rand(9)" )]
        [DataRow( "Rand(5)" )]
        public void Rand_Fixed_RandomValueIsReturned( string arguments )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<double>( $"SELECT {arguments};" );
            scalar.Should( ).BeGreaterThan( 0 );
            scalar.Should( ).BeLessThan( 1 );
        }

        [DataTestMethod]
        [DataRow("ASCII(null)", null)]
        [DataRow("ASCII('')", null)]
        [DataRow( "ASCII('t')", 116 )]
        [DataRow( "ASCII('techonthenet.com')", 116 )]
        [DataRow( "ASCII('T')", 84 )]
        [DataRow( "ASCII('TechOnTheNet.com')", 84 )]
        public void ASCII_Fixed_ValueIsReturned( string arguments, int? expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<byte?>( $"SELECT {arguments};" );
            scalar.Should( ).Be( (byte?)expected );
        }

        [TestMethod]
        public void ASCII_FixedNoParameters_ThrowsException(  )
        {
            using var connection = new MemoryDbConnection( );
            Action act = () => connection.Execute( "SELECT ASCII();" );
            act.Should( ).Throw<SqlInvalidFunctionParameterCountException>( );
        }

        [DataTestMethod]
        [DataRow("CHAR(null)", null)]
        [DataRow("CHAR('')", null)]
        [DataRow( "CHAR(116)", 't' )]
        [DataRow( "CHAR(84)", 'T' )]
        public void Char_Fixed_ValueIsReturned( string arguments, char? expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<char?>( $"SELECT {arguments};" );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("CHARINDEX('t', 'TechOnTheNet.com')", 1)]
        [DataRow("CHARINDEX('t', 'TechOnTheNet.com', 2)", 7)]
        [DataRow( "CHARINDEX('t', 'TechOnTheNet.com', 8)", 12 )]
        [DataRow( "CHARINDEX('ON', 'TechOnTheNet.com')", 5 )]
        [DataRow( "CHARINDEX('z', 'TechOnTheNet.com')", 0 )]
        public void CharIndex_Fixed_ValueIsReturned( string arguments, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( $"SELECT {arguments};" );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT CONCAT('TechOnTheNet', '.com');", "TechOnTheNet.com")]
        [DataRow("SELECT CONCAT('Tech', 'On', 'The', 'Net', '.com');", "TechOnTheNet.com")]
        [DataRow("SELECT CONCAT('Tech ', 'On ', 'The ', 'Net');", "Tech On The Net")]
        [DataRow("SELECT CONCAT('Orange', ' ', 'Peach');", "Orange Peach")]
        [DataRow("SELECT CONCAT('Let', '''', 's learn SQL Server')", "Let's learn SQL Server")]
        public void Concat_Fixed_ValueIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT DATALENGTH(' ');", 1)]
        [DataRow("SELECT DATALENGTH('');", 0)]
        [DataRow("SELECT DATALENGTH('TechOnTheNet.com');", 16 )]
        [DataRow("SELECT DATALENGTH('   TechOnTheNet.com   ');", 22 )]
        [DataRow("SELECT DATALENGTH(1234);", 4)]
        [DataRow("SELECT DATALENGTH('2004-05-01');", 10 )]
        [DataRow("SELECT DATALENGTH(NULL);", null )]
        public void Datalength_Fixed_LengthIsReturned( string sql, int? expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int?>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow( "SELECT LEFT('TechOnTheNet.com', 12);", "TechOnTheNet" )]
        [DataRow( "SELECT LEFT('TechOnTheNet.com', 4);", "Tech" )]
        [DataRow( "SELECT LEFT('Tech On The Net', 8);", "Tech On " )]
        [DataRow( "SELECT LEFT('Tech On The Net', 100);", "Tech On The Net" )]
        public void Left_Fixed_ValueIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT LEN(' ');", 0)]
        [DataRow("SELECT LEN('');", 0)]
        [DataRow("SELECT LEN('TechOnTheNet.com   ');", 16 )]
        [DataRow("SELECT LEN('   TechOnTheNet.com');", 19 )]
        [DataRow("SELECT LEN('   TechOnTheNet.com   ');", 19 )]
        [DataRow("SELECT LEN(NULL);", null )]
        public void Len_Fixed_ValueIsReturned( string sql, int? expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int?>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT LOWER('TECHONTHENET.COM');", "techonthenet.com")]
        [DataRow("SELECT LOWER('TechOnTheNet.com');", "techonthenet.com")]
        [DataRow("SELECT LOWER('Tech on the Net');", "tech on the net")]
        public void Lower_Fixed_ValueIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT LTRIM('   TechOnTheNet.com');", "TechOnTheNet.com")]
        [DataRow("SELECT LTRIM('   TechOnTheNet.com   ');", "TechOnTheNet.com   ")]
        [DataRow("SELECT LTRIM('   Tech on the Net');", "Tech on the Net")]
        public void Ltrim_Fixed_ValueIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("NCHAR(null)", null)]
        [DataRow("NCHAR('')", null)]
        [DataRow("NCHAR(116)", 't' )]
        [DataRow("NCHAR(84)", 'T' )]
        public void NChar_Fixed_ValueIsReturned( string arguments, char? expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<char?>( $"SELECT {arguments};" );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT PATINDEX('%onthe%', 'TechOnTheNet.com');", 5)]
        [DataRow("SELECT PATINDEX('%T_e%', 'TechOnTheNet.com');", 7)]
        [DataRow("SELECT PATINDEX('%e%com', 'TechOnTheNet.com');", 2)]
        [DataRow("SELECT PATINDEX('%[aeiou]%', 'TechOnTheNet.com');", 2)]
        [DataRow("SELECT PATINDEX('%z%', 'TechOnTheNet.com');", 0)]
        public void PatIndex_Fixed_IndexIsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT REPLACE('TechOnTheNet.com', 'T', '3');", "3echOn3heNe3.com")]
        [DataRow("SELECT REPLACE('TechOnTheNet.com', 'Tech', '1234');", "1234OnTheNet.com")]
        [DataRow("SELECT REPLACE('TechOnTheNet.com', 'c', '123');", "Te123hOnTheNet.123om")]
        [DataRow("SELECT REPLACE('Tech On The Net', ' ', 'Z');", "TechZOnZTheZNet")]
        public void Replace_Fixed_ValueIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( sql );
            scalar.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT RIGHT('TechOnTheNet.com', 12);", "OnTheNet.com")]
        [DataRow("SELECT RIGHT('TechOnTheNet.com', 4);", ".com")]
        [DataRow("SELECT RIGHT('Tech On The Net', 8);", " The Net")]
        [DataRow("SELECT RIGHT('Tech On The Net', 100);", "Tech On The Net")]
        public void Right_Fixed_ValueIsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( sql );
            scalar.Should( ).Be( expected );
        }


    }
}
