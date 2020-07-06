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
    public class MiscellaneousTests
    {
        [TestMethod]
        public void VariableAtAtVersion_Fixed_IsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<string>( "SELECT @@Version" );
            value.Should( ).StartWith( "SQL Memory Database V" );
        }

        [TestMethod]
        public void CurrentUser_Fixed_IsReturned(  )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<string>( "SELECT CURRENT_USER" );
            value.Should( ).Be( "dbo" );
        }

        [DataTestMethod]
        [DataRow("SELECT ISDATE('2014-05-01');", 1)]
        [DataRow("SELECT ISDATE('2014-05-01 10:03');", 1)]
        [DataRow("SELECT ISDATE('2014-05-01 10:03:32');", 1)]
        [DataRow("SELECT ISDATE('2014-05-01 10:03:32.001');", 1)]
        [DataRow("SELECT ISDATE('techonthenet.com');", 0)]
        [DataRow("SELECT ISDATE(123);", 0)]
        public void IsDate_Fixed_IsReturned( string sql, int isDate )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<int>( sql );
            value.Should( ).Be( isDate );
        }

        [DataTestMethod]
        [DataRow("SELECT ISNULL(NULL, 'TechOnTheNet.com');", "TechOnTheNet.com")]
        [DataRow("SELECT ISNULL('CheckYourMath.com', 'TechOnTheNet.com');", "CheckYourMath.com")]
        public void NullString_Fixed_IsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<string>( sql );
            value.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT ISNULL(NULL, 45);", 45)]
        [DataRow("SELECT ISNULL(12, 45);", 12)]
        public void NullInt_Fixed_IsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<int>( sql );
            value.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT COALESCE(NULL, NULL, 'TechOnTheNet.com', NULL, 'CheckYourMath.com');", "TechOnTheNet.com")]
        [DataRow("SELECT COALESCE(NULL, 'TechOnTheNet.com', 'CheckYourMath.com');", "TechOnTheNet.com")]
        public void CoalesceString_Fixed_IsReturned( string sql, string expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<string>( sql );
            value.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT COALESCE(NULL, NULL, 1, 2, 3, NULL, 4);", 1)]
        public void CoalesceInt_Fixed_IsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<int>( sql );
            value.Should( ).Be( expected );
        }

        [DataTestMethod]
        [DataRow("SELECT ISNUMERIC(1234);", 1)]
        [DataRow("SELECT ISNUMERIC('1234');", 1)]
        [DataRow("SELECT ISNUMERIC('techonthenet.com');", 0)]
        [DataRow("SELECT ISNUMERIC('2014-05-01');", 0)]
        public void IsNumeric_Fixed_IsReturned( string sql, int expected )
        {
            using var connection = new MemoryDbConnection( );
            var value = connection.ExecuteScalar<int>( sql );
            value.Should( ).Be( expected );
        }

        [TestMethod]
        public void Lead_Fixed_ResultIsReturned( )
        {
            const string sql = @"SELECT dept_id, last_name, salary,
LEAD (salary,1) OVER (ORDER BY salary) AS next_highest_salary
FROM employees;";

            using var connection = new MemoryDbConnection( );
            connection.Execute( SqlStatements.SqlLeadExamples );
            var employees = connection.Query<EmployeeLeadDto>( sql );
        }
    }
}
