DROP DATABASE IF EXISTS course_kafka_2022prices;
CREATE DATABASE IF NOT EXISTS course_kafka_2022;
USE course_kafka_2022;

DROP TABLE IF EXISTS Offsets;
CREATE TABLE Offsets
(
    `consumer`  varchar(120) NOT NULL,
    `topic`     varchar(120) NOT NULL,
    `partition` int          NOT NULL,
    `offset`    bigint,
    PRIMARY KEY (`consumer`, `topic`, `partition`)
);

DROP TABLE IF EXISTS Prices;
CREATE TABLE Prices
(
    id        int          NOT NULL AUTO_INCREMENT,
    symbol    varchar(5)   NOT NULL,
    name      varchar(255) NOT NULL,
    price     float        NOT NULL,
    timestamp datetime     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

