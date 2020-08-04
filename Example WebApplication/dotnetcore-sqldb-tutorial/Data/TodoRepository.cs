using Dapper;
using DotNetCoreSqlDb.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DotNetCoreSqlDb.Data
{
    public class TodoRepository : ITodoRepository
    {
        private readonly IDbConnectionFactory _ConnectionFactory;

        public TodoRepository( IDbConnectionFactory factory )
        {
            _ConnectionFactory = factory;
        }

        public async Task<IEnumerable<Todo>> GetAllAsync( )
        {
            await using var connection = _ConnectionFactory.Create(  );
            return await connection.QueryAsync<Todo>( "SELECT ID, Description, CreatedDate, Version FROM Todo");    
        }

        public async Task<Todo> GetByIdAsync( int id )
        {
            await using var connection = _ConnectionFactory.Create(  );
            return await connection.QuerySingleOrDefaultAsync<Todo>( "SELECT ID, Description, CreatedDate, Version FROM Todo WHERE ID = @id", new { id } );    
        }

        public async Task<int> StoreAsync( Todo todo )
        {
            var sql = todo.ID == 0
                ? "INSERT into Todo (Description, CreatedDate) VALUES (@Description, @CreatedDate)"
                : "UPDATE Todo SET Description = @Description, CreatedDate=@CreatedDate WHERE ID = @ID AND Version = @Version";
            await using var connection = _ConnectionFactory.Create(  );
            return await connection.ExecuteAsync( sql, todo );    
        }

        public async Task<int> DeleteAsync( int id, byte[] version )
        {
            await using var connection = _ConnectionFactory.Create(  );
            return await connection.ExecuteAsync( "DELETE FROM Todo WHERE ID = @id AND Version = @version", new { id, version } );    
        }
    }
}
