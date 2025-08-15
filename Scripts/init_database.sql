
--------------------------####  Project DataWareHousing ###-----------------------------------


/*
	PURPOSE -- check wheather the database already exist , create the database , create the schemas as bronze, silver, golden layers
  warning -- Running this script will delete the whole database , if exist , ensure to have backup
*/

USE master;
GO

--- CHECKING THE DATABASE EXIST OR NOT , DROP AND RECREATE IF EXIST

IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse') 
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO


---- CREATING THE DATABASE 

CREATE DATABASE DataWareHouse;
USE DataWareHouse;




--- CREATING THE SCHEMAS FOR BRONZE, SILVER, & GOLD LAYERS

CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO

--- GO - SEPARATE BATCHES WHEN WORKING WITH MULTIPLE SQL STATEMENT



