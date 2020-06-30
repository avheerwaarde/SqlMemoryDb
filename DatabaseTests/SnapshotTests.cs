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
    public class SnapshotTests
    {
        private const string _SqlSelectApplication = "SELECT [Name],[User],[DefName] FROM application";
        private const string _SqlInsertApplication = "INSERT INTO application ([Name],[User],[DefName]) VALUES (@Name, @User, @DefName)";
        private const string _SqlUpdateApplication = "UPDATE application SET [Name] = N'New Name'";

        [TestMethod]
        public async Task Transaction_Commit_DataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );
            var rows = connection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            var currentRowCount = rows.Count;

            await connection.OpenAsync( );
            var transaction = await connection.BeginTransactionAsync( );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"}, transaction );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"}, transaction );
            await transaction.CommitAsync( );
            var currentRows = connection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            currentRows.Count.Should( ).Be( currentRowCount + 2 );
        }

        [TestMethod]
        public async Task Transaction_Rollback_NoDataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );
            var rows = connection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            var currentRowCount = rows.Count;

            await connection.OpenAsync( );
            var transaction = await connection.BeginTransactionAsync( );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"}, transaction );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"}, transaction );
            await transaction.RollbackAsync( );
            var currentRows = connection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            currentRows.Count.Should( ).Be( currentRowCount );
        }

        [TestMethod]
        public async Task Transaction_CommitUpdate_DataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );

            await connection.OpenAsync( );
            var transaction = await connection.BeginTransactionAsync( );
            await connection.ExecuteAsync( _SqlUpdateApplication, transaction );
            await transaction.CommitAsync( );
            var applications = await connection.QueryAsync<ApplicationDto>( _SqlSelectApplication );
            foreach ( var application in applications )
            {
                application.Name.Should().Be( "New Name" );
            }
        }

        [TestMethod]
        public async Task Transaction_RollbackUpdate_NoDataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );

            await connection.OpenAsync( );
            var transaction = await connection.BeginTransactionAsync( );
            await connection.ExecuteAsync( _SqlUpdateApplication, transaction );
            await transaction.RollbackAsync( );
            var applications = await connection.QueryAsync<ApplicationDto>( _SqlSelectApplication );
            foreach ( var application in applications )
            {
                application.Name.Should().Be( "Name String" );
            }
        }

        [TestMethod]
        public async Task Snapshot_SaveRemove_DataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );
            var memoryDatabase = connection.GetMemoryDatabase( );
            memoryDatabase.SaveSnapshot(  );
            var rows = memoryDatabase.Tables[ "dbo.application" ].Rows;
            var currentRowCount = rows.Count;


            await connection.OpenAsync( );
            var transaction = await connection.BeginTransactionAsync( );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"}, transaction );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"}, transaction );
            await transaction.CommitAsync( );

            memoryDatabase.RemoveSnapshot(  );
            var currentRows = memoryDatabase.Tables[ "dbo.application" ].Rows;
            currentRows.Count.Should( ).Be( currentRowCount + 2 );
        }

        [TestMethod]
        public async Task Snapshot_SaveRestore_NoDataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );
            var memoryDatabase = connection.GetMemoryDatabase( );
            memoryDatabase.SaveSnapshot(  );
            var rows = memoryDatabase.Tables[ "dbo.application" ].Rows;
            var currentRowCount = rows.Count;

            await connection.OpenAsync( );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"} );
            await connection.ExecuteAsync( _SqlInsertApplication, new {Name = "Name string", User = "User string", DefName = "DefName string"} );
            memoryDatabase.RestoreSnapshot(  );
            var currentRows = memoryDatabase.Tables[ "dbo.application" ].Rows;
            currentRows.Count.Should( ).Be( currentRowCount );
        }

        [TestMethod]
        public async Task Snapshot_SaveRemoveUpdate_DataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );
            var memoryDatabase = connection.GetMemoryDatabase( );
            memoryDatabase.SaveSnapshot(  );
            
            await connection.OpenAsync( );
            await connection.ExecuteAsync( _SqlUpdateApplication );

            memoryDatabase.RemoveSnapshot(  );
            var applications = await connection.QueryAsync<ApplicationDto>( _SqlSelectApplication );
            foreach ( var application in applications )
            {
                application.Name.Should().Be( "New Name" );
            }
        }

        [TestMethod]
        public async Task Snapshot_SaveRestoreUpdate_NoDataIsCommitted( )
        {
            await SqlScripts.InitDbAsync( );

            await using var connection = new MemoryDbConnection( );
            var memoryDatabase = connection.GetMemoryDatabase( );
            memoryDatabase.SaveSnapshot(  );
            
            await connection.OpenAsync( );
            await connection.ExecuteAsync( _SqlUpdateApplication );

            memoryDatabase.RestoreSnapshot(  );
            var applications = await connection.QueryAsync<ApplicationDto>( _SqlSelectApplication );
            foreach ( var application in applications )
            {
                application.Name.Should().Be( "Name String" );
            }
        }

    }
}
