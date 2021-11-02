DROP TABLE IF EXISTS Prices;
CREATE TABLE Offsets (
      [consumer] varchar(120) NOT NULL,
      [topic] varchar(120) NOT NULL,
      [partition] int NOT NULL,
      [offset] bigint
);
ALTER TABLE myTable
    ADD  PRIMARY KEY (consumer, topic, partition)
GO;
