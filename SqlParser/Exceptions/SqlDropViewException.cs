using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlDropViewException : Exception
    {
        public SqlDropViewException( string name )
            : base( $"Cannot drop the view '{name}', because it does not exist or you do not have permission." )
        {

        }
    }
}
