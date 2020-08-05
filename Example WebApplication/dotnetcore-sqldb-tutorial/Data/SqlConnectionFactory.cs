using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data.Common;

namespace DotNetCoreSqlDb.Data
{
    public class SqlConnectionFactory : IDbConnectionFactory
    {
        private readonly string _ConnectionString;

        public SqlConnectionFactory( IConfiguration configuration )
        {
            _ConnectionString = configuration.GetConnectionString( "MyDbConnection" );
        }

        public DbConnection Create( )
        {
            return new SqlConnection( _ConnectionString );
        }
    }
}
