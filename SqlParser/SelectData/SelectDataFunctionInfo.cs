using System;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFunctionInfo
    {
        public Type SelectType;
        public int MinimalArgumentCount;
        public Type ReturnType;
        public string ReturnDbType;
    }
}