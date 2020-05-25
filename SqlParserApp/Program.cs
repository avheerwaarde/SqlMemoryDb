using System;
using System.Data;
using System.Data.SqlClient;
using SqlParser;

namespace SqlParserApp
{
    class Program
    {
        private const string _InitDb = @"
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
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]";

        static void Main( string[] args )
        {
            var info = new SqlMetaInfo();
            info.InitializeDb( _InitDb );
        }
    }
}
