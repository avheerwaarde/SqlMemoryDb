using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlFunctionNotSupportedException : Exception
    {
        public SqlFunctionNotSupportedException( string method ) : base( $"'{method}' is not a recognized built-in function name." )
        {

        }
    }
}
