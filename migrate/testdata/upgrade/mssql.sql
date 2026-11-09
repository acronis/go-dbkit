-- SQL Server export of go-dbkit fixture tables, generated from sys catalog metadata and rows.
CREATE TABLE [dbo].[migrations] (
    [id] varchar(255) NOT NULL,
    [applied_at] datetime NULL,
    PRIMARY KEY ([id])
);
GO
INSERT INTO [dbo].[migrations] ([id], [applied_at]) VALUES (N'1_create_users', CONVERT(DATETIME, '2026-11-09T14:27:02.267', 126));
GO
INSERT INTO [dbo].[migrations] ([id], [applied_at]) VALUES (N'2_seed_users', CONVERT(DATETIME, '2026-11-09T14:27:03.403', 126));
GO
INSERT INTO [dbo].[migrations] ([id], [applied_at]) VALUES (N'3_create_audit', CONVERT(DATETIME, '2026-11-09T14:27:04.523', 126));
GO
CREATE TABLE [dbo].[legacy_users] (
    [id] int NOT NULL,
    [name] nvarchar(100) NOT NULL,
    PRIMARY KEY ([id])
);
GO
INSERT INTO [dbo].[legacy_users] ([id], [name]) VALUES (1, N'alice');
GO
INSERT INTO [dbo].[legacy_users] ([id], [name]) VALUES (2, N'Zoë 東京');
GO
INSERT INTO [dbo].[legacy_users] ([id], [name]) VALUES (3, N'untouched');
GO
CREATE TABLE [dbo].[legacy_audit] (
    [id] int NOT NULL,
    [note] nvarchar(100) NOT NULL,
    PRIMARY KEY ([id])
);
GO
INSERT INTO [dbo].[legacy_audit] ([id], [note]) VALUES (1, N'created by 3');
GO
