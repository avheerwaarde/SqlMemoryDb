using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidCastException : Exception
    {
        public SqlInvalidCastException( string toType )
            : base( $"Conversion failed when converting to {toType}." )
        {

        }
    }
}
