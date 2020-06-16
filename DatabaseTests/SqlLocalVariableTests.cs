using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using SqlMemoryDb.Exceptions;

namespace DatabaseTests
{
    [TestClass]
    public class SqlLocalVariableTests
    {
        [TestMethod]
        public void LocalVariable_SetFromLiteral_ValueIsSet( )
        {
            const string sql = @"
DECLARE @Id int
SET @Id = 9
SELECT @Id";
            using var connection = new MemoryDbConnection( );
            var variableId = connection.ExecuteScalar<int>( sql );
            variableId.Should( ).Be( 9 );
        }

        [TestMethod]
        public void LocalVariable_SetFromInvalidLiteral_ExceptionIsThrown( )
        {
            const string sql = @"
DECLARE @Id int
SET @Id = N'fail me'
SELECT @Id";
            using var connection = new MemoryDbConnection( );
            Func<int> act = ( ) => connection.ExecuteScalar<int>( sql );
            act.Should( ).Throw<FormatException>( );
        }

        [TestMethod]
        public void LocalVariable_SetFromGlobalVariable_ValueIsSet( )
        {
            const string sql = @"
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
DECLARE @Id int
SET @Id = @@IDENTITY
SELECT @Id";

            MemoryDbConnection.GetMemoryDatabase( ).Clear(  );
            using var connection = new MemoryDbConnection( );
            var variableId = connection.ExecuteScalar<int>( SqlStatements.SqlCreateTableApplication + "\n" + sql );
            variableId.Should( ).Be( 4 );
        }

        [TestMethod]
        public void LocalVariable_SetFromUnknownGlobalVariable_ThrowsException( )
        {
            const string sql = @"
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
DECLARE @Id int
SET @Id = @@UNKNOWN
SELECT @Id";

            MemoryDbConnection.GetMemoryDatabase( ).Clear(  );
            using var connection = new MemoryDbConnection( );
            Func<int> act = ( ) => connection.ExecuteScalar<int>( SqlStatements.SqlCreateTableApplication + "\n" + sql );
            act.Should( ).Throw<SqlInvalidParameterNameException>( );
        }

        [TestMethod]
        public void LocalVariable_SetFromMethod_ValueIsSet( )
        {
            const string sql = @"
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
DECLARE @Id int
SET @Id = Scope_Identity()
SELECT @Id";

            MemoryDbConnection.GetMemoryDatabase( ).Clear(  );
            using var connection = new MemoryDbConnection( );
            var variableId = connection.ExecuteScalar<int>( SqlStatements.SqlCreateTableApplication + "\n" + sql );
            variableId.Should( ).Be( 4 );
        }

        [TestMethod]
        public void LocalVariable_SetFromUnknownMethod_ThrowsException( )
        {
            const string sql = @"
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
DECLARE @Id int
SET @Id = Unknown()
SELECT @Id";

            MemoryDbConnection.GetMemoryDatabase( ).Clear(  );
            using var connection = new MemoryDbConnection( );
            Func<int> act = ( ) => connection.ExecuteScalar<int>( SqlStatements.SqlCreateTableApplication + "\n" + sql );
            act.Should( ).Throw<SqlFunctionNotSupportedException>( );
        }

        [TestMethod]
        public void LocalVariable_SetFromSelect_ValueIsSet( )
        {
            const string sql = @"
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')
DECLARE @Id int
SET @Id = (SELECT MAX(Id) from application)
SELECT @Id";

            MemoryDbConnection.GetMemoryDatabase( ).Clear(  );
            using var connection = new MemoryDbConnection( );
            var variableId = connection.ExecuteScalar<int>( SqlStatements.SqlCreateTableApplication + "\n" + sql );
            variableId.Should( ).Be( 4 );
        }

    }
}
