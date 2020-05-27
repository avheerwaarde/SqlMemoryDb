using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class ExecuteNonQueryTests
    {
        const string _SqlCreateTable = @"
CREATE TABLE [dbo].[application](
	[Id] int IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]";

        [TestMethod]
        public async Task OpenConnection_CreateTable_Ok( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = _SqlCreateTable;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            var db = MemoryDbConnection.GetMemoryDatabase( );
            db.Tables.Count.Should( ).Be( 1 );
        }
    }
}
