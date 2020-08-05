CREATE TABLE [dbo].[Todo] (
    [ID]          INT            IDENTITY (1, 1) NOT NULL,
    [Description] NVARCHAR (MAX) NULL,
    [CreatedDate] DATETIME2 (7)  NOT NULL,
    [Version] ROWVERSION NOT NULL, 
    CONSTRAINT [PK_Todo] PRIMARY KEY CLUSTERED ([ID] ASC)
);

