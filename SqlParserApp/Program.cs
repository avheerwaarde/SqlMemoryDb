using System;
using SqlParser;

namespace SqlParserApp
{
    class Program
    {
        private const string _InitDb = @"
CREATE TABLE [dbo].[application_feature](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[name] [nvarchar](50) NOT NULL,
	[is_active] [bit] NOT NULL DEFAULT(1),
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
