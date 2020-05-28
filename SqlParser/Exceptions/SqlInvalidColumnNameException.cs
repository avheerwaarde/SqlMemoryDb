using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidColumnNameException: Exception
    {
        public SqlInvalidColumnNameException( string columnName ) : base( $"Invalid column with name { columnName ?? "" }" )
        {

        }

    }
}
