using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class FunctionConversionTests
    {
        [TestMethod]
        public void Cast_Fixed_IntIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( "SELECT CAST(14.85 AS int);" );
            scalar.Should( ).Be( 14 );
        }

        [TestMethod]
        public void Cast_Fixed_FloatIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<double>( "SELECT CAST(14.85 AS float);" );
            scalar.Should( ).Be( 14.85 );
        }

        [TestMethod]
        public void Cast_Fixed_StringIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( "SELECT CAST(14.85 AS varchar);" );
            scalar.Should( ).Be( "14.85" );
        }

        [TestMethod]
        public void Cast_Fixed_String4IsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( "SELECT CAST(15.6 AS varchar(4));" );
            scalar.Should( ).Be( "15.6" );
        }

        [TestMethod]
        public void CastString_Fixed_FloatIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<double>( "SELECT CAST('14.85' AS float);" );
            scalar.Should( ).Be( 14.85 );
        }

        [TestMethod]
        public void CastStringToDate_Fixed_DateIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var date = connection.ExecuteScalar<DateTime>( "SELECT CAST('2020-04-05' AS datetime);" );
            date.Should( ).Be( new DateTime( 2020, 4, 5 ) );
        }

        [TestMethod]
        public void CastStringInvalidToDate_Fixed_ExceptionIsThrows(  )
        {
            using var connection = new MemoryDbConnection( );
            Action action = () => connection.ExecuteScalar<DateTime>( "SELECT CAST('Invalid' AS datetime);" );
            action.Should( ).Throw<SqlInvalidCastException>( );
        }

        [TestMethod]
        public void TryCastStringInvalidToDate_Fixed_NullIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var date = connection.ExecuteScalar<DateTime?>( "SELECT TRY_CAST('Invalid' AS datetime);" );
            date.Should( ).BeNull( );
        }

        [TestMethod]
        public void Convert_Fixed_IntIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<int>( "SELECT CONVERT(int, 14.85);" );
            scalar.Should( ).Be( 14 );
        }

        [TestMethod]
        public void ConvertDateWithFormat_Fixed_StringIsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var scalar = connection.ExecuteScalar<string>( "SELECT CONVERT(varchar, '2014-02-15', 101)" );
            scalar.Should( ).Be( "15/02/2014" );
        }
    }
}
