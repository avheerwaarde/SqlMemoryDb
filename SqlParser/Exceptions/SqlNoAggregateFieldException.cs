using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlNoAggregateFieldException : Exception
    {
        public SqlNoAggregateFieldException( string field ) 
            : base( $"Column '{field}' is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause." )
        {
        }
    }
}
