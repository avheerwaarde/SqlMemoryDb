using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseTests
{
    internal class SqlStatements
    {
        public const string SqlCreateTableApplication = @"
CREATE TABLE [dbo].[application](
	[Id] int IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]";


    }
}
