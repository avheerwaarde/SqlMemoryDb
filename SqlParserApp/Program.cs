using System;
using System.Data;
using System.Data.SqlClient;
using SqlMemoryDb;
using SqlParser;

namespace SqlParserApp
{
    class Program
    {
        private const string _InitDb = @"
CREATE TABLE [dbo].[application](
	[Id] int IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE TABLE [dbo].[application_feature](
	[Id] int IDENTITY(1,1) NOT NULL,
	[bit] [bit] NULL,
	[char] [char](10) NULL,
	[varchar] [varchar](10) NULL,
	[nchar] [nchar](10) NULL,
	[nvarchar] [nvarchar](10) NULL,
	[nvarchar_max] [nvarchar](max) NULL,
	[byte] [tinyint] NULL,
	[byte_array] [varbinary](50) NULL,
	[numeric] [numeric](18, 0) NULL,
	[int] [int] NULL,
	[bigint] [bigint] NULL,
	[fk_application] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
CONSTRAINT [FK_application_feature_feature] FOREIGN KEY ([fk_application]) REFERENCES [application]([Id]) ON DELETE CASCADE ON UPDATE CASCADE
) ON [PRIMARY]

CREATE TABLE[dbo].[application_action]
(

   [Id] int IDENTITY(1,1) NOT NULL,
   [Name] [nvarchar](max) NULL,
   [Action][nvarchar] (max) NULL,
   [fk_application][int] NULL,
PRIMARY KEY CLUSTERED
(
   [Id] ASC
)WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY]
) ON[PRIMARY]

ALTER TABLE [dbo].[application_action] WITH CHECK ADD CONSTRAINT [FK_application_action_application] FOREIGN KEY([fk_application])
REFERENCES [dbo].[application] ([Id])
";

        static void Main( string[] args )
        {
            using ( SqlConnection conn = new SqlConnection("Data Source=(local);Initial Catalog=TestMemoryDb;Integrated Security=True;" ) )
            {
                conn.Open( );

                // Get the Meta Data for Supported Schema Collections
                //DataTable tableDataTable = conn.GetSchema( "Tables" );
                //ShowDataTable( tableDataTable );
                DataTable columnDataTable = conn.GetSchema( "Columns" );
                ShowDataTable( columnDataTable, 30 );
            }


            var info = new MemoryDatabase();
            info.ExecuteSqlStatement( _InitDb );
            foreach ( var table in info.Tables )
            {
                Console.WriteLine( table.Key );
            }
        }

   private static void ShowDataTable(DataTable table, Int32 length) {
      foreach (DataColumn col in table.Columns) {
         Console.Write("{0,-" + length + "}", col.ColumnName);
      }
      Console.WriteLine();

      foreach (DataRow row in table.Rows) {
         foreach (DataColumn col in table.Columns) {
            if (col.DataType.Equals(typeof(DateTime)))
               Console.Write("{0,-" + length + ":d}", row[col]);
            else if (col.DataType.Equals(typeof(Decimal)))
               Console.Write("{0,-" + length + ":C}", row[col]);
            else
               Console.Write("{0,-" + length + "}", row[col]);
         }
         Console.WriteLine();
      }
   }

   private static void ShowDataTable(DataTable table) {
      ShowDataTable(table, 14);
   }

   private static void ShowColumns(DataTable columnsTable) {
      var selectedRows = from info in columnsTable.AsEnumerable()
                         select new {
                            TableCatalog = info["TABLE_CATALOG"],
                            TableSchema = info["TABLE_SCHEMA"],
                            TableName = info["TABLE_NAME"],
                            ColumnName = info["COLUMN_NAME"],
                            DataType = info["DATA_TYPE"]
                         };

      Console.WriteLine("{0,-15}{1,-15}{2,-15}{3,-15}{4,-15}", "TableCatalog", "TABLE_SCHEMA",
          "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE");
      foreach (var row in selectedRows) {
         Console.WriteLine("{0,-15}{1,-15}{2,-15}{3,-15}{4,-15}", row.TableCatalog,
             row.TableSchema, row.TableName, row.ColumnName, row.DataType);
      }
   }

   private static void ShowIndexColumns(DataTable indexColumnsTable) {
      var selectedRows = from info in indexColumnsTable.AsEnumerable()
                         select new {
                            TableSchema = info["table_schema"],
                            TableName = info["table_name"],
                            ColumnName = info["column_name"],
                            ConstraintSchema = info["constraint_schema"],
                            ConstraintName = info["constraint_name"],
                            KeyType = info["KeyType"]
                         };

      Console.WriteLine("{0,-14}{1,-11}{2,-14}{3,-18}{4,-16}{5,-8}", "table_schema", "table_name", "column_name", "constraint_schema", "constraint_name", "KeyType");
      foreach (var row in selectedRows) {
         Console.WriteLine("{0,-14}{1,-11}{2,-14}{3,-18}{4,-16}{5,-8}", row.TableSchema,
             row.TableName, row.ColumnName, row.ConstraintSchema, row.ConstraintName, row.KeyType);
      }
   }
    }
}
