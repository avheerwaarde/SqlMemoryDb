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
            var info = new MemoryDatabase();
            info.ExecuteSqlStatement( _InitDb );
            foreach ( var table in info.Tables )
            {
                Console.WriteLine( table.Key );
            }
        }
    }
}
