using System;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;
using System.Threading.Tasks;

namespace DatabaseTests
{
    [TestClass]
    public class CommandTests
    {
        [TestMethod]
        public async Task New_Create_Ok()
        {
            await using var connection = new MemoryDbConnection( );
            var command = connection.CreateCommand( );
            command.Should( ).BeOfType<MemoryDbCommand>( );
            command.Parameters.Count.Should( ).Be( 0 );
        }

        [TestMethod]
        public async Task Prepare_OpenedConnection_Ok()
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            await command.PrepareAsync(  );
        }

        [TestMethod]
        public async Task Prepare_NewConnection_FailConnectionShouldSet()
        {
            var command = new MemoryDbCommand( null, null, null );
            Func<Task> act = async () => { await command.PrepareAsync( ); };
            await act.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("The Connection should be set.");
        }

        [TestMethod]
        public async Task Prepare_UnopenedConnection_FailConnectionShouldBeOpen()
        {
            await using var connection = new MemoryDbConnection( );
            var command = connection.CreateCommand( );
            Func<Task> act = async () => { await command.PrepareAsync( ); };
            await act.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("The Connection should be opened.");
        }

        [TestMethod]
        public async Task CreateParameter_New_Ok()
        {
            await using var connection = new MemoryDbConnection( );
            var command = connection.CreateCommand( );
            var parameter = command.CreateParameter( );
            parameter.Should( ).BeOfType<MemoryDbParameter>( );
        }


    }
}
