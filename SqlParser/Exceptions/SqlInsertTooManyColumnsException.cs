using System;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInsertTooManyColumnsException: Exception
    {
        public SqlInsertTooManyColumnsException( ) : base( "There are more columns in the INSERT statement than values specified in the VALUES clause. The number of values in the VALUES clause must match the number of columns specified in the INSERT statement." )
        {

        }
    }
}
