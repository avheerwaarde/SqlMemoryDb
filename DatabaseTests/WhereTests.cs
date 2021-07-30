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

namespace DatabaseTests
{
    [TestClass]
    public class WhereTests
    {
        [DataTestMethod]
        [DataRow( null, 3 )]
        [DataRow( "Name1", 1 )]
        [DataRow( "Name2", 1 )]
        [DataRow( "Not found", 0 )]
        public void WhereClause_OptionalTest_ShouldFilter( string name, int count )
        {
            var applications = new ApplicationDto[]
            {
                new ApplicationDto { Name = "Name1", User = "User1", DefName = "Def1"},
                new ApplicationDto { Name = "Name2", User = "User2", DefName = "Def2"},
                new ApplicationDto { Name = "Name3", User = "User3", DefName = "Def3"},
            };
            const string sqlInsert = "INSERT INTO application ([Name], [User], [DefName]) VALUES (@Name, @User, @DefName)";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlStatements.SqlCreateTableApplication );
            connection.Execute( sqlInsert, applications );

            string sqlSelect = $"{SqlStatements.SqlSelectApplication} WHERE @name is NULL OR @name = [Name]";
            var appsQueried = connection.Query<ApplicationDto>( sqlSelect, new { name = name } ).ToList();
            appsQueried.Count.Should().Be( count );
            if ( count == 1 )
            {
                appsQueried.First().Name.Should().Be( name );
            }
        }

        [DataTestMethod]
        [DataRow( null, 3 )]
        [DataRow( "Name1", 1 )]
        [DataRow( "Name2", 1 )]
        [DataRow( "Not found", 0 )]
        public void WhereClause_OptionalTestLike_ShouldFilter( string name, int count )
        {
            var applications = new ApplicationDto[]
            {
                new ApplicationDto { Name = "Name1", User = "User1", DefName = "Def1"},
                new ApplicationDto { Name = "Name2", User = "User2", DefName = "Def2"},
                new ApplicationDto { Name = "Name3", User = "User3", DefName = "Def3"},
            };
            const string sqlInsert = "INSERT INTO application ([Name], [User], [DefName]) VALUES (@Name, @User, @DefName)";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlStatements.SqlCreateTableApplication );
            connection.Execute( sqlInsert, applications );

            string sqlSelect = $"{SqlStatements.SqlSelectApplication} WHERE @name is NULL OR [Name] LIKE @name";
            var appsQueried = connection.Query<ApplicationDto>( sqlSelect, new { name = name } ).ToList();
            appsQueried.Count.Should().Be( count );
            if ( count == 1 )
            {
                appsQueried.First().Name.Should().Be( name );
            }
        }

        [TestMethod]
        public void WhereClause_IntParameterWithCalculation_ShouldFilter( )
        {
            var applications = new ApplicationDto[]
            {
                new ApplicationDto { Name = "Name1", User = "User1", DefName = "Def1"},
                new ApplicationDto { Name = "Name2", User = "User2", DefName = "Def2"},
                new ApplicationDto { Name = "Name3", User = "User3", DefName = "Def3"},
            };
            const string sqlInsert = "INSERT INTO application ([Name], [User], [DefName]) VALUES (@Name, @User, @DefName)";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlStatements.SqlCreateTableApplication );
            connection.Execute( sqlInsert, applications );

            string sqlSelect = $"{SqlStatements.SqlSelectApplication} WHERE Id = @id + 1";
            int id = 0;
            var appsQueried = connection.Query<ApplicationDto>( sqlSelect, new { id } ).ToList();
            appsQueried.Count.Should().Be( 1 );
            appsQueried.First().Id.Should().Be( id + 1 );
        }


    }
}
