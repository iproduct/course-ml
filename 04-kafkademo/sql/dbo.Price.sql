DROP TABLE IF EXISTS Prices;
CREATE TABLE Prices (
       [id] int IDENTITY(1,1) PRIMARY KEY,
       [symbol] varchar(5) NOT NULL,
       [name] varchar(255) NOT NULL,
       [price] float NOT NULL,
       [timestamp] datetime NOT NULL DEFAULT getdate()
);
