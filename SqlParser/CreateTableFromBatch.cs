using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb
{
    class CreateTableFromBatch
    {
        public Table ToDatabase( SqlObjectIdentifier identifier, MemoryDbConnection connection,  MemoryDbDataReader.ResultBatch batch )
        {
            var table = new Table( identifier );
            foreach ( var field in batch.Fields )
            {
                Column column;
                if ( ( field as MemoryDbDataReader.ReaderFieldData )?.SelectFieldData is SelectDataFromColumn select )
                {
                    column = new Column( select.TableColumn.Column, field.Name, table.Columns.Count )
                    {
                        ParentTable = table, 
                        ComputedExpression = null
                    };
                }
                else
                {
                    var sqlType = Helper.DbType2SqlType( field.DbType );
                    column = new Column( table, field.Name, sqlType, connection.GetMemoryDatabase(  ).UserDataTypes, table.Columns.Count );
                }
                table.Columns.Add( column );
            }

            if ( Helper.IsLocalTempTable( identifier ) )
            {
                connection.TempTables.Add( table.FullName, table );
            }
            else
            {
                connection.MemoryDatabase.Tables.Add( table.FullName, table );
            }
            return table;
        }
    }
}
