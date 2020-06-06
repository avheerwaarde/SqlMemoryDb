using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb
{
    internal class RawData
    {
        public class RawDataRow
        {
            public string Name;
            public Table Table;
            public ArrayList Row;
        }
        public List<List<RawDataRow>> RawRowList = new List<List<RawDataRow>>();
        public DbParameterCollection Parameters { get; set; }
        public SqlBooleanExpression HavingClause { get; set; }
        public SqlOrderByItemCollection SortOrder { get; set; }

        public Dictionary<string,Table> TableAliasList = new Dictionary<string, Table>();
        public List<TableColumn> GroupByFields = new List<TableColumn>();
    }
}