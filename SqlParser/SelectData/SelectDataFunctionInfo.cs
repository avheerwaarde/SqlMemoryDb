using System;
using System.Data;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFunctionInfo
    {
        public Type SelectType;
        public int MinimalArgumentCount;
        public Type ReturnType;
        public DbType? ReturnDbType;
    }
}