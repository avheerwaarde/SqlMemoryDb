using System;
using System.Data;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class ConnectionTests
    {
        [TestMethod]
        public void Connection_New_Ok()
        {
            var connection = new MemoryDbConnection( );
            connection.State.Should( ).Be( ConnectionState.Closed );
        }

        [TestMethod]
        public async Task Connection_NewAsync_Ok()
        {
            await using var connection = new MemoryDbConnection( );
            connection.State.Should( ).Be( ConnectionState.Closed );
        }

        [TestMethod]
        public void Connection_NotOpened_OpenOk()
        {
            var connection = new MemoryDbConnection( );
            connection.Open(  );
            connection.State.Should( ).Be( ConnectionState.Open );
            connection.Database.Should( ).Be( "Memory" );
        }

        [TestMethod]
        public async Task Connection_NotOpened_OpenAsyncOk()
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync(  );
            connection.State.Should( ).Be( ConnectionState.Open );
            connection.Database.Should( ).Be( "Memory" );
        }

        [TestMethod]
        public void Connection_AlreadyOpened_OpenFails()
        {
            var connection = new MemoryDbConnection( );
            connection.Open(  );
            
            Action act = () => connection.Open(  );
            act.Should().Throw<InvalidOperationException>()
                .WithMessage("The connection is already open. Current state = 'Open'");
        }

        [TestMethod]
        public void Connection_AlreadyOpened_CloseOk()
        {
            var connection = new MemoryDbConnection( );
            connection.Open(  );
            connection.Close(  );
            connection.State.Should( ).Be( ConnectionState.Closed );
        }

        [TestMethod]
        public async Task  Connection_AlreadyOpened_CloseAsyncOk()
        {
            await using var connection = new MemoryDbConnection( );
            connection.Open(  );
            await connection.CloseAsync(  );
            connection.State.Should( ).Be( ConnectionState.Closed );
        }

        [TestMethod]
        public void Connection_NotOpened_CloseOk()
        {
            var connection = new MemoryDbConnection( );
            Action act = () => connection.Close(  );
            connection.State.Should( ).Be( ConnectionState.Closed );
        }

        [TestMethod]
        public void Connection_CreateCommand_Ok()
        {
            var connection = new MemoryDbConnection( );
            connection.Open(  );
            var command = connection.CreateCommand( );
            command.Should( ).BeOfType<MemoryDbCommand>( );
            command.Connection.Should( ).Be( connection );
        }

    }
}
