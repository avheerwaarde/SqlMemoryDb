using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlUnionExpressionCount : Exception
    {
        public SqlUnionExpressionCount()
            : base ("All queries combined using a UNION, INTERSECT or EXCEPT operator must have an equal number of expressions in their target lists.")
        {
        }
    }
}
