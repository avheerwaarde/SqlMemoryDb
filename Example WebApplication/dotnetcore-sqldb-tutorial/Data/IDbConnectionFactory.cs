using System.Data.Common;
using System.Threading.Tasks;

namespace DotNetCoreSqlDb.Data
{
    public interface IDbConnectionFactory
    {
        DbConnection Create( );
    }
}
