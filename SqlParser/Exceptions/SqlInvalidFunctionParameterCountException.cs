using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidFunctionParameterCountException : Exception
    {
        public SqlInvalidFunctionParameterCountException( string methodName, int expected )
            : base( $"The {methodName} function requires {expected} argument(s)" )
        {

        }
    }
}
