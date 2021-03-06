﻿using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using System;
using System.Threading.Tasks;

namespace DatabaseTests
{
    [TestClass]
    public class ScalarTests
    {
        [DataTestMethod]
        [DataRow( "GETDATE()", "DateTime", typeof(DateTime) )]
        [DataRow("1", "bit", typeof(bool))]
        [DataRow("99", "byte", typeof(byte))]
        [DataRow("99", "numeric", typeof(decimal))]
        [DataRow("99", "int", typeof(int))]
        public async Task ExecuteScalar_ByType_TypeShouldBeCorrect( string setValue, string fieldName, Type expectedType )
        {
            string sqlInsert = $"INSERT INTO application_feature ([{fieldName}]) VALUES ({setValue})";
            string sqlSelect = $"SELECT [{fieldName}] FROM application_feature";

            await using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            await connection.ExecuteAsync( SqlStatements.SqlCreateTableApplication + "\n" 
                                                                                   + SqlStatements.SqlCreateTableApplicationFeature );
            await connection.ExecuteAsync( sqlInsert );
            var value = await connection.ExecuteScalarAsync( sqlSelect );
            value.Should( ).BeOfType( expectedType );
        }

        [DataTestMethod]
        [DataRow("ident_current('application_feature')")]
        [DataRow("@@identity")]
        [DataRow("SCOPE_IDENTITY()")]
        public async Task ExecuteScalar_Identity_IdentityIsReturned( string identityMethod )
        {
            string sqlInsert = $"INSERT INTO application_feature ([int]) VALUES (99)";
            string sqlSelect = $"SELECT {identityMethod}";

            await using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            await connection.ExecuteAsync( SqlStatements.SqlCreateTableApplication + "\n" 
                                                                                   + SqlStatements.SqlCreateTableApplicationFeature );
            await connection.ExecuteAsync( sqlInsert );
            await connection.ExecuteAsync( sqlInsert );
            await connection.ExecuteAsync( sqlInsert );
            var value = await connection.ExecuteScalarAsync( sqlSelect );
            value.Should( ).Be( 3 );
        }
    }
}
