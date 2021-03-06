﻿using System;
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
    public class InsertTableTests
    {
        [TestInitialize]
        public async Task InitializeDb( )
        {
            await using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication + "\n" 
                                + SqlStatements.SqlCreateTableApplicationFeature + "\n" 
                                + SqlStatements.SqlCreateTableApplicationAction ;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
        }
        
        [TestMethod]
        public async Task InsertApplication_CorrectSql_RowIsAdded( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            var rows = connection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            rows.Count.Should( ).Be( 1, "A new row should be added" );
            var row = rows.First( );
            row[ 0 ].Should( ).Be( 1 );
            row[ 1 ].Should( ).Be( "Name" );
            row[ 2 ].Should( ).Be( "User" );
            row[ 3 ].Should( ).Be( "DefName" );
        }

        [TestMethod]
        public async Task InsertApplication_TooManyColumns_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInsertTooManyColumnsException>( );
        }

        [TestMethod]
        public async Task InsertApplication_TooManyValues_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInsertTooManyValuesException>( );
        }

        [TestMethod]
        public async Task InsertApplication_MaxLengthExceeded_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefNameDefNameDefNameDefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlDataTruncatedException>( );
        }

        [TestMethod]
        public async Task InsertApplication_InvalidColumnName_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[Unknown]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInvalidColumnNameException>( );
        }

        [TestMethod]
        public async Task InsertApplication_MissingRequiredColumn_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[DefName]) VALUES (N'Name', N'DefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlFieldIsNullException>( );
        }

        [TestMethod]
        public async Task InsertApplication_IdentityColumn_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Id],[Name],[User],[DefName]) VALUES (3, N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInsertIdentityException>( );
        }

        [TestMethod]
        public async Task InsertApplication_TableWithCorrectForeignKey_RowIsAdded( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            command.CommandText = "INSERT INTO application_action ([fk_application]) VALUES (1)";
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            var rowsParent = connection.GetMemoryDatabase( ).Tables[ "dbo.application" ].Rows;
            rowsParent.Count.Should( ).Be( 1, "A new row should be added" );
            var rows = connection.GetMemoryDatabase( ).Tables[ "dbo.application_action" ].Rows;
            rows.Count.Should( ).Be( 1, "A new row should be added" );
        }

        [TestMethod]
        public async Task InsertApplication_TableWithInvalidForeignKey_ThrowsException( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name', N'User', N'DefName')";
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            command.CommandText = "INSERT INTO application_action ([fk_application]) VALUES (3)";
            await command.PrepareAsync( );
            Func<Task> act = async () => { await command.ExecuteNonQueryAsync( ); };
            await act.Should( ).ThrowAsync<SqlInsertInvalidForeignKeyException>( );
        }

        [TestMethod]
        public async Task Insert_Select_RowsAreInserted( )
        {
            await SqlScripts.InitDbAsync( );

            using var connection = new MemoryDbConnection( );
            connection.Execute( "INSERT INTO TextTable ([Text]) SELECT [Name] FROM application_action WHERE [Order] = 2" );
            var texts = connection.Query<TextDto>( $"SELECT [Text] FROM TextTable" ).ToList(  );
            texts.Count.Should( ).Be( 3 );
        }
    }
}
