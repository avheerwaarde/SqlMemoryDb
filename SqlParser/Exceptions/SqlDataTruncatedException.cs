using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlDataTruncatedException : Exception
    {
        public SqlDataTruncatedException( int max, int actual ) 
            : base( $"String or binary data would be truncated. Actual length of {actual} exceeds maximum allowed length of {max}." )
        {

        }
    }
}
