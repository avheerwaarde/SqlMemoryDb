using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlNoScalarResultException : Exception
    {
        public SqlNoScalarResultException( ) :
            base( "ExecuteScalarSql() only allows no result or a single value from a single row.")
        {

        }
    }
}
