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
    public class ExistsTests
    {
        private const string _SqlIfExists = @"
IF NOT EXISTS (SELECT 1 FROM [DbScriptsRun] where [ScriptId] = N'69AA00A3-9C2D-4C87-81B5-3BE55CC384F6')
BEGIN
	INSERT [dbo].[DbScriptsRun] ([ScriptId],[DateInserted]) VALUES ( N'69AA00A3-9C2D-4C87-81B5-3BE55CC384F6', CURRENT_TIMESTAMP)
END";

        private const string _SqlIfExists2 = @"
IF NOT EXISTS (SELECT 1 FROM [DbScriptsRun] where [ScriptId] = N'A790157D-B1A2-4621-BEC3-7CD03127F3B4')
BEGIN
	INSERT [dbo].[DbScriptsRun] ([ScriptId],[DateInserted]) VALUES ( N'A790157D-B1A2-4621-BEC3-7CD03127F3B4', CURRENT_TIMESTAMP)
END";

        [TestMethod]
        public void IfExists_NoEntries_RowAdded( )
        {
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            connection.Execute( SqlStatements.SqlCreateDbScriptsRun + "\n" + _SqlIfExists );
            var runScripts = connection.Query<DbScriptRunDto>( SqlStatements.SqlSelectDbScriptRun ).ToList();
            runScripts.Count.Should( ).Be( 1 );
        }

        [TestMethod]
        public void IfExists_ExistingSameEntry_NoRowAdded( )
        {
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            connection.Execute( SqlStatements.SqlCreateDbScriptsRun + "\n" + _SqlIfExists );
            connection.Execute( _SqlIfExists );
            var runScripts = connection.Query<DbScriptRunDto>( SqlStatements.SqlSelectDbScriptRun ).ToList();
            runScripts.Count.Should( ).Be( 1 );
        }

        [TestMethod]
        public void IfExists_ExistingOtherEntry_RowAdded( )
        {
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            connection.Execute( SqlStatements.SqlCreateDbScriptsRun + "\n" + _SqlIfExists );
            connection.Execute( _SqlIfExists2 );
            var runScripts = connection.Query<DbScriptRunDto>( SqlStatements.SqlSelectDbScriptRun ).ToList();
            runScripts.Count.Should( ).Be( 2 );
        }
    }
}
