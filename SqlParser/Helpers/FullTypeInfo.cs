using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SqlMemoryDb.Helpers
{
    class FullTypeInfo
    {
        public DbType? DbDataType { get ; set ; }
        public Type NetDataType { get ; set ; }
    }
}
