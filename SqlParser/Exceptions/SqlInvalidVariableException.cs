using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidVariableException : Exception
    {
        public SqlInvalidVariableException( string parameterName ) 
            : base( $"Must declare the scalar variable '{parameterName}'" )
        {

        }
    }
}
