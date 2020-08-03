using System;

namespace SqlMemoryDb.Exceptions
{
    public class SqlUpdateColumnForbiddenException : Exception
    {
        public SqlUpdateColumnForbiddenException( string columnName ) 
            : base( $"Column value may not be set for column '{columnName}'." )
        {

        }
    }
}
