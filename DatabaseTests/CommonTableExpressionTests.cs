using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class CommonTableExpressionTests
    {
        [TestMethod]
        public void CommonTableExpression_Simple_Succeeds(  )
        {
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase(  ).Clear(  );
            connection.Execute( SqlScriptAdventureWorks.InitTables );
        }
    }
}
