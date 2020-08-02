using System;
using System.Threading.Tasks;
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
            connection.Execute( _SqlCreateTable );
        }

        [TestMethod]
        public void Insert_Normal_RowWithVersionIsInserted( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlInsertTable, new { Description = "Dummy", CreatedDate = DateTime.Now } );
            var todo = connection.QuerySingle( _SqlSelectTable );
            var version = VersionAsLong( todo.Version );
            
            AssertionExtensions.Should( version ).Be( 0x7d1 );
        }

        [TestMethod]
        public async Task Insert_SetRowVersion_ThrowsException( )
        {
            const string sqlInsertTable = @"INSERT into Todo (Description, CreatedDate,Version) VALUES (@Description, @CreatedDate,@Version)";
            var version = BitConverter.GetBytes( ( long ) 0x123 );

            await using var connection = new MemoryDbConnection( );
            Func<Task> act = async () => { await connection.ExecuteAsync( sqlInsertTable, new { Description = "Dummy", CreatedDate = DateTime.Now, Version = version } ); };
            await act.Should( ).ThrowAsync<InvalidOperationException>( );
        }

        [TestMethod]
        public void Update_Normal_RowWithVersionIsIncremented( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlInsertTable, new { Description = "Dummy", CreatedDate = DateTime.Now } );
            var todo = connection.QuerySingle( _SqlSelectTable );
            var version1 = VersionAsLong( todo.Version );
            connection.Execute( _SqlUpdateTable, (object)todo );
            var todo2 = connection.QuerySingle( _SqlSelectTable );
            var version2 = VersionAsLong( todo2.Version );
            AssertionExtensions.Should( version2 ).BeGreaterThan( version1 );
        }

        [TestMethod]
        public void Update_WrongRowVersion_RowNotUpdated( )
        {
            using var connection = new MemoryDbConnection( );
            connection.Execute( _SqlInsertTable, new { Description = "Dummy", CreatedDate = DateTime.Now } );
            var todo = connection.QuerySingle( _SqlSelectTable );
            var affected1 = connection.Execute( _SqlUpdateTable, (object)todo );
            affected1.Should( ).Be( 1 );
            var affected2 = connection.Execute( _SqlUpdateTable, (object)todo );
            affected2.Should( ).Be( 0 );
        }
    }
}
