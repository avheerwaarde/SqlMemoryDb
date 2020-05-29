using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidParameterNameException : Exception
    {
        public SqlInvalidParameterNameException( string name ) : base( $"Invalid parameter with name { name ?? "" }" )
        {

        }
    }
}
