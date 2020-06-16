using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidObjectNameException : Exception
    {
        public SqlInvalidObjectNameException( string name )
            : base( $"Invalid object name '{name}'." )
        {

        }
    }
}
