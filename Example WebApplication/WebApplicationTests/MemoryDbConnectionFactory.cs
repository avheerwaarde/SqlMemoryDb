using DotNetCoreSqlDb.Data;
using SqlMemoryDb;
using System.Data.Common;

namespace WebApplicationTests
{
    public class MemoryDbConnectionFactory : IDbConnectionFactory
    {
        public DbConnection Create( )
        {
            return new MemoryDbConnection(  );
        }
    }
}
