```sql
-- /var/opt/mssql/data/test.mdf

ALTER DATABASE test ADD FILEGROUP p_fg0;  
ALTER DATABASE test ADD FILEGROUP p_fg1;  
ALTER DATABASE test ADD FILEGROUP p_fg2;  
ALTER DATABASE test ADD FILEGROUP p_fg3;  
ALTER DATABASE test ADD FILEGROUP p_fg4;  
ALTER DATABASE test ADD FILEGROUP p_fg5;  
ALTER DATABASE test ADD FILEGROUP p_fg6;  
ALTER DATABASE test ADD FILEGROUP p_fg7;  
ALTER DATABASE test ADD FILEGROUP p_fg8;  
ALTER DATABASE test ADD FILEGROUP p_fg9;  
ALTER DATABASE test ADD FILEGROUP p_fga;  
ALTER DATABASE test ADD FILEGROUP p_fgb;  
ALTER DATABASE test ADD FILEGROUP p_fgc;  
ALTER DATABASE test ADD FILEGROUP p_fgd;  
ALTER DATABASE test ADD FILEGROUP p_fge;  
ALTER DATABASE test ADD FILEGROUP p_fgf;
go

ALTER DATABASE test   
ADD FILE (NAME = p_dat0, FILENAME = '/var/opt/mssql/data/p_dat0.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg0;
ALTER DATABASE test   
ADD FILE (NAME = p_dat1, FILENAME = '/var/opt/mssql/data/p_dat1.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg1;
ALTER DATABASE test   
ADD FILE (NAME = p_dat2, FILENAME = '/var/opt/mssql/data/p_dat2.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg2;
ALTER DATABASE test   
ADD FILE (NAME = p_dat3, FILENAME = '/var/opt/mssql/data/p_dat3.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg3;
ALTER DATABASE test   
ADD FILE (NAME = p_dat4, FILENAME = '/var/opt/mssql/data/p_dat4.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg4;
ALTER DATABASE test   
ADD FILE (NAME = p_dat5, FILENAME = '/var/opt/mssql/data/p_dat5.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg5;
ALTER DATABASE test   
ADD FILE (NAME = p_dat6, FILENAME = '/var/opt/mssql/data/p_dat6.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg6;
ALTER DATABASE test   
ADD FILE (NAME = p_dat7, FILENAME = '/var/opt/mssql/data/p_dat7.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg7;
ALTER DATABASE test   
ADD FILE (NAME = p_dat8, FILENAME = '/var/opt/mssql/data/p_dat8.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg8;
ALTER DATABASE test   
ADD FILE (NAME = p_dat9, FILENAME = '/var/opt/mssql/data/p_dat9.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fg9;
ALTER DATABASE test   
ADD FILE (NAME = p_data, FILENAME = '/var/opt/mssql/data/p_data.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fga;
ALTER DATABASE test   
ADD FILE (NAME = p_datb, FILENAME = '/var/opt/mssql/data/p_datb.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fgb;
ALTER DATABASE test   
ADD FILE (NAME = p_datc, FILENAME = '/var/opt/mssql/data/p_datc.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fgc;
ALTER DATABASE test   
ADD FILE (NAME = p_datd, FILENAME = '/var/opt/mssql/data/p_datd.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fgd;
ALTER DATABASE test   
ADD FILE (NAME = p_date, FILENAME = '/var/opt/mssql/data/p_date.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fge;
ALTER DATABASE test   
ADD FILE (NAME = p_datf, FILENAME = '/var/opt/mssql/data/p_datf.ndf', SIZE = 5MB, MAXSIZE = UNLIMITED, FILEGROWTH = 5MB )  
TO FILEGROUP p_fgf;
go

CREATE PARTITION FUNCTION p_range_fn0 (binary(16))  
AS RANGE RIGHT FOR VALUES (
  0x1000000000000000,
  0x2000000000000000,
  0x3000000000000000,
  0x4000000000000000,
  0x5000000000000000,
  0x6000000000000000,
  0x7000000000000000,
  0x8000000000000000,
  0x9000000000000000,
  0xA000000000000000,
  0xB000000000000000,
  0xC000000000000000,
  0xD000000000000000,
  0xE000000000000000,
  0xF000000000000000
)
go

CREATE PARTITION SCHEME p_range_ps0  
    AS PARTITION p_range_fn0  
    TO (p_fg0, p_fg1, p_fg2, p_fg3, p_fg4, p_fg5, p_fg6, p_fg7, p_fg8, p_fg9, p_fga, p_fgb, p_fgc, p_fgd, p_fge, p_fgf) 
go

CREATE TABLE kv (container binary(16), ty varchar(50), ref binary(16)) 
    ON p_range_ps0 (container)  
GO

-- Premise that we are moving from non-partitioned to partitioned.
CREATE TABLE pre_kv (container binary(16), ty varchar(50), ref binary(16)) 
go

declare @ct int = 1000
while @ct > 0 begin
insert pre_kv select convert(binary(16),newid()), 'type1', convert(binary(16),newid())
select @ct = @ct - 1
end
go

-- Check distribution
select 
p.partition_number partition_number,
f.name partition_filegroup, 
p.rows num_rows 
from sys.partitions p join sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
join sys.filegroups f ON dds.data_space_id = f.data_space_id
WHERE OBJECT_NAME(OBJECT_ID) = 'kv'

/*
create table kv (container binary(16), ty varchar(50), ref binary(16), primary key (container, ty, ref));
insert kv select 0xBD2108171260B04FB8191272954E7F40, 'type1', convert(binary(16),newid());
insert kv select 0xBD2108171260B04FB8191272954E7F40, 'type1', convert(binary(16),newid());
insert kv select 0xBD2108171260B04FB8191272954E7F40, 'type1', convert(binary(16),newid());
insert kv select 0xFF1E726082D8A84AAEEFFF69B391BC40, 'type1', convert(binary(16),newid());
insert kv select 0xFF1E726082D8A84AAEEFFF69B391BC40, 'type1', convert(binary(16),newid());
insert kv select 0xFF1E726082D8A84AAEEFFF69B391BC40, 'type1', convert(binary(16),newid());
*/

```