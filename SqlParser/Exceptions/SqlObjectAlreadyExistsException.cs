using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlObjectAlreadyExistsException : Exception
    {
        public SqlObjectAlreadyExistsException( string name )
            : base( $"There is already an object named '{name}' in the database." )
        {

        }
    }
}
