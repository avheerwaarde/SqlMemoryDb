using System;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class RowVersionTests
    {
        private const string _SqlCreateTable = @"
CREATE TABLE [dbo].[Todo](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Description] [nvarchar](max) NULL,
	[CreatedDate] [datetime2](7) NOT NULL,
	[Version] [rowversion] NOT NULL,
 CONSTRAINT [PK_Todo] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]";

        private const string _SqlSelectTable = @"SELECT ID, Description, CreatedDate, Version FROM Todo";
        private const string _SqlInsertTable = @"INSERT into Todo (Description, CreatedDate) VALUES (@Description, @CreatedDate)";
        private const string _SqlUpdateTable = @"UPDATE Todo SET Description = @Description, CreatedDate=@CreatedDate WHERE ID = @ID AND Version = @Version";

        public static long VersionAsLong( byte[] version )
        {
            if ( BitConverter.IsLittleEndian )
            {
                Array.Reverse( version );
            }

            return BitConverter.ToInt64( version );
        }

        [TestInitialize]
        public void InitializeDb( )
        {
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
        }

        [TestMethod]
        public void CreateInsertSelect_Normal_RowWithVersionIsInserted( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlCreateTable );
            connection.Execute( _SqlInsertTable, new { Description = "Dummy", CreatedDate = DateTime.Now } );
            var todo = connection.QuerySingle( _SqlSelectTable );
            var version = VersionAsLong( todo.Version );
            
            AssertionExtensions.Should( version ).Be( 0x7d1 );
        }

    }
}
