using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetCoreSqlDb.Models;

namespace DotNetCoreSqlDb.Data
{
    public interface ITodoRepository
    {
        Task<IEnumerable<Todo>> GetAllAsync( );
        Task<Todo> GetByIdAsync( int id );
        Task<int> StoreAsync( Todo todo );
        Task<int> DeleteAsync( int id, byte[] modifiedDate );
    }
}
