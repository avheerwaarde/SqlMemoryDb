using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlFieldIsNullException : Exception
    {
        public SqlFieldIsNullException( string table, string column ) : base( $"Cannot insert the value NULL into column '{column}', table '{table}'; column does not allow nulls. INSERT fails." )
        {

        }
    }
}
