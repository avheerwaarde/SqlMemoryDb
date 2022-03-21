using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class UniqueIdentifierTests
    {

        [TestMethod]
        public void WhereClause_WithGuid_ShouldFilter(  )
        {
            var entities = new UniqueDto[]
            {
                new UniqueDto { Unique = Guid.NewGuid() },
                new UniqueDto { Unique = Guid.NewGuid() },
                new UniqueDto { Unique = Guid.NewGuid() },
            };
            const string sqlInsert = "INSERT INTO UniqueTable ([Unique]) VALUES (@Unique)";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlStatements.SqlCreateTableUniqueIdentifier );
            connection.Execute( sqlInsert, entities );

            string sqlSelect = $"SELECT [Unique] FROM UniqueTable WHERE [Unique] = @Unique";
            var tableQueried = connection.QuerySingle<UniqueDto>( sqlSelect, entities[1] );
            tableQueried.Unique.Should().Be( entities[1].Unique );
        }

        [TestMethod]
        public void WhereClause_WithGuidAsText_ShouldFilter()
        {
            var entities = new UniqueDto[]
            {
                new UniqueDto { Unique = Guid.NewGuid() },
                new UniqueDto { Unique = Guid.NewGuid() },
                new UniqueDto { Unique = Guid.NewGuid() },
            };
            const string sqlInsert = "INSERT INTO UniqueTable ([Unique]) VALUES (@Unique)";

            using var connection = new MemoryDbConnection();
            connection.GetMemoryDatabase().Clear();
            connection.Execute( SqlStatements.SqlCreateTableUniqueIdentifier );
            connection.Execute( sqlInsert, entities );

            string sqlSelect = $"SELECT [Unique] FROM UniqueTable WHERE [Unique] = @Unique";
            var tableQueried = connection.QuerySingle<UniqueDto>( sqlSelect, new { Unique = entities[1].Unique.ToString() }  );
            tableQueried.Unique.Should().Be( entities[ 1 ].Unique );
        }
    }
}
