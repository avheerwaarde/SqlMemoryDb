using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlUnqualifiedColumnNameException : Exception
    {
        public SqlUnqualifiedColumnNameException( string columnName ) : base( $"No table specified for column with name { columnName ?? "" }" )
        {

        }

    }
}
