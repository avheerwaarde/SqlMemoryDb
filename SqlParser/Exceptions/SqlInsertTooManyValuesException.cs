using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInsertTooManyValuesException: Exception
    {
        public SqlInsertTooManyValuesException( ) : base( "There are fewer columns in the INSERT statement than values specified in the VALUES clause. The number of values in the VALUES clause must match the number of columns specified in the INSERT statement." )
        {

        }
    }
}
