USE master
go


create OR ALTER PROCEDURE sp_helpindex_dba
	@objname nvarchar(776)		
as

	set nocount on

	declare @objid int,			
			@indid smallint,	
			@groupid int,  		
			@indname sysname,
			@groupname sysname,
			@status int,
			@keys nvarchar(2126),	
			@inc_columns	nvarchar(max),
			@inc_Count		smallint,
			@loop_inc_Count		smallint,
			@dbname	sysname,
			@ignore_dup_key	bit,
			@is_unique		bit,
			@is_hypothetical	bit,
			@is_primary_key	bit,
			@is_unique_key 	bit,
			@auto_created	bit,
			@no_recompute	bit,
			@filter_definition	nvarchar(max),

			@type_desc VARCHAR(500), 
			@data_compression_desc VARCHAR(500),  
			@IndexSizeGB NUMERIC(12,2), 
			@user_seeks bigint, 
			@last_user_seek datetime, 
			@user_scans BIGINT, 
			@last_user_scan datetime, 
			@user_lookups BIGINT, 
			@Total_User_Escrita BIGINT,  
			@Total_User_Leitura BIGINT, 
			@Dif_Read_Write BIGINT,
			@allow_row_locks BIT, 
			@row_lock_count BIGINT,  
			@row_lock_wait_count BIGINT, 
			@row_lock_wait_in_ms BIGINT, 
			@allow_page_locks BIT, 
			@page_lock_count bigint, 
			@page_lock_wait_count bigint,  
			@page_lock_wait_in_ms bigint, 
			@fill_factor int, 
			@is_padded BIT


	select @dbname = parsename(@objname,3)
	if @dbname is null
		select @dbname = db_name()
	else if @dbname <> db_name()
		begin
			raiserror(15250,-1,-1)
			return (1)
		end

	
	select @objid = object_id(@objname)
	if @objid is NULL
	BEGIN
		raiserror(15009,-1,-1,@objname,@dbname)
		return (1)
	end

	-- Lista índices da tabela
	declare ms_crs_ind cursor local static for
		
		SELECT 
				ix.index_id, ix.data_space_id, ix.name,
				ix.ignore_dup_key, ix.is_unique, ix.is_hypothetical, ix.is_primary_key, ix.is_unique_constraint,
				s.auto_created, s.no_recompute, ix.filter_definition,
				ix.type_desc, t.data_compression_desc, IndexSizeGB, vw.user_seeks, vw.last_user_seek, vw.user_scans, vw.last_user_scan, vw.user_lookups, vw.user_updates as 'Total_User_Escrita',(vw.user_scans + vw.user_seeks + vw.user_lookups) as 'Total_User_Leitura',vw.user_updates - (vw.user_scans + vw.user_seeks + vw.user_lookups) as 'Dif_Read_Write',
				ix.allow_row_locks, vwx.row_lock_count, row_lock_wait_count, row_lock_wait_in_ms,ix.allow_page_locks, vwx.page_lock_count, page_lock_wait_count, page_lock_wait_in_ms, ix.fill_factor, ix.is_padded -- INTO tab_info_index 
		from sys.indexes ix 
		LEFT JOIN sys.dm_db_index_usage_stats vw on ix.index_id = vw.index_id and ix.object_id = vw.object_id AND vw.database_id = DB_ID()
		LEFT JOIN sys.dm_db_index_operational_stats(db_id(), null, NULL, NULL) vwx on vwx.index_id = ix.index_id and ix.object_id = vwx.object_id
		left join (SELECT i.[name] AS IndexName, i.object_id, p.data_compression_desc
		,((SUM(s.[used_page_count]) * 8) /1024 )/1024. AS IndexSizeGB
		FROM sys.dm_db_partition_stats AS s
		INNER JOIN sys.indexes AS i ON s.[object_id] = i.[object_id]    AND s.[index_id] = i.[index_id] 
		inner join sys.partitions p on p.partition_id = s.partition_id
		GROUP BY i.[name],p.data_compression_desc, i.object_id) t on t.IndexName = ix.name AND t.object_id = ix.object_id
		inner JOIN sys.stats s on ix.object_id = s.object_id and ix.index_id = s.stats_id
		where ix.object_id = @objid 

	OPEN ms_crs_ind
	FETCH ms_crs_ind INTO @indid, @groupid, @indname, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @auto_created, @no_recompute, @filter_definition, 
			@type_desc, @data_compression_desc, @IndexSizeGB, @user_seeks, @last_user_seek, @user_scans, @last_user_scan, @user_lookups, @Total_User_Escrita,  @Total_User_Leitura, @Dif_Read_Write,
			@allow_row_locks, @row_lock_count, @row_lock_wait_count, @row_lock_wait_in_ms, @allow_page_locks, @page_lock_count, @page_lock_wait_count, @page_lock_wait_in_ms, @fill_factor, @is_padded

			PRINT @indid

	
	if @@fetch_status < 0
	begin
		deallocate ms_crs_ind
		raiserror(15472,-1,-1,@objname) 
		return (0)
	end

	CREATE TABLE #tabind
	(
		index_name				sysname	collate database_default NOT NULL,
		index_id				int,
		ignore_dup_key			BIT,
		is_unique				BIT,
		is_hypothetical			BIT,
		is_primary_key			BIT,
		is_unique_key			BIT,
		auto_created			bit,
		no_recompute			BIT,
		groupname				sysname collate database_default NULL,
		index_keys				NVARCHAR(2126)	collate database_default NOT NULL, -- see @keys above for length descr
		filter_definition		NVARCHAR(max),
		inc_Count				smallint,
		inc_columns				NVARCHAR(max),
		[type_desc]				[NVARCHAR](60) NULL,
		[data_compression_desc] [nvarchar](60) NULL,
		[IndexSizeGB]			[NUMERIC](25, 6) NULL,
		[user_seeks]			[BIGINT] NULL,
		[last_user_seek]		[DATETIME] NULL,
		[user_scans]			[BIGINT] NULL,
		[last_user_scan]		[DATETIME] NULL,
		[user_lookups]			[BIGINT] NULL,
		[Total_User_Escrita]	[bigint] NULL,
		[Total_User_Leitura]	[bigint] NULL,
		[Dif_Read_Write]		[BIGINT] NULL,
		[allow_row_locks]		[BIT] NULL,
		[row_lock_count]		[BIGINT] NULL,
		[row_lock_wait_count]	[bigint] NULL,
		[row_lock_wait_in_ms]	[BIGINT] NULL,
		[allow_page_locks]		[BIT] NULL,
		[page_lock_count]		[BIGINT] NULL,
		[page_lock_wait_count]	[BIGINT] NULL,
		[page_lock_wait_in_ms]	[BIGINT] NULL,
		[fill_factor]			[TINYINT] NOT NULL,
		[is_padded]				[BIT] NULL
	)



	CREATE TABLE #IncludedColumns
	(	RowNumber	smallint,
		[Name]	nvarchar(128)
	)

	while @@fetch_status >= 0
	begin
		declare @i int, @thiskey nvarchar(131) 

		select @keys = index_col(@objname, @indid, 1), @i = 2
		if (indexkey_property(@objid, @indid, 1, 'isdescending') = 1)
			select @keys = @keys  + '(-)'

		select @thiskey = index_col(@objname, @indid, @i)
		if ((@thiskey is not null) and (indexkey_property(@objid, @indid, @i, 'isdescending') = 1))
			select @thiskey = @thiskey + '(-)'

		while (@thiskey is not null )
		begin
			select @keys = @keys + ', ' + @thiskey, @i = @i + 1
			select @thiskey = index_col(@objname, @indid, @i)
			if ((@thiskey is not null) and (indexkey_property(@objid, @indid, @i, 'isdescending') = 1))
				select @thiskey = @thiskey + '(-)'
		end

		
		SELECT @inc_Count = count(*)
		FROM
		sys.tables AS tbl
		INNER JOIN sys.indexes AS si 
			ON (si.index_id > 0 
				and si.is_hypothetical = 0) 
				AND (si.object_id=tbl.object_id)
		INNER JOIN sys.index_columns AS ic 
			ON (ic.column_id > 0 
				and (ic.key_ordinal > 0 or ic.partition_ordinal = 0 or ic.is_included_column != 0)) 
				AND (ic.index_id=CAST(si.index_id AS int) AND ic.object_id=si.object_id)
		INNER JOIN sys.columns AS clmns 
			ON clmns.object_id = ic.object_id 
			and clmns.column_id = ic.column_id
		WHERE ic.is_included_column = 1 and
			(si.index_id = @indid) and 
			(tbl.object_id= @objid)

		IF @inc_Count > 0
		BEGIN
			
			DELETE FROM #IncludedColumns
			INSERT #IncludedColumns
				SELECT ROW_NUMBER() OVER (ORDER BY clmns.column_id) 
				, clmns.name 
			FROM
			sys.tables AS tbl
			INNER JOIN sys.indexes AS si 
				ON (si.index_id > 0 
					and si.is_hypothetical = 0) 
					AND (si.object_id=tbl.object_id)
			INNER JOIN sys.index_columns AS ic 
				ON (ic.column_id > 0 
					and (ic.key_ordinal > 0 or ic.partition_ordinal = 0 or ic.is_included_column != 0)) 
					AND (ic.index_id=CAST(si.index_id AS int) AND ic.object_id=si.object_id)
			INNER JOIN sys.columns AS clmns 
				ON clmns.object_id = ic.object_id 
				and clmns.column_id = ic.column_id
			WHERE ic.is_included_column = 1 and
				(si.index_id = @indid) and 
				(tbl.object_id= @objid)
			
			SELECT @inc_columns = [Name] FROM #IncludedColumns WHERE RowNumber = 1

			SET @loop_inc_Count = 1

			WHILE @loop_inc_Count < @inc_Count
			BEGIN
				SELECT @inc_columns = @inc_columns + ', ' + [Name] 
					FROM #IncludedColumns WHERE RowNumber = @loop_inc_Count + 1
				SET @loop_inc_Count = @loop_inc_Count + 1
			END
		END
		-- SELECT @indname, @inc_columns
		select @groupname = null
		select @groupname = name from sys.data_spaces where data_space_id = @groupid


		insert into #tabind values (@indname, @indid, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @auto_created, @no_recompute, @groupname, @keys, @filter_definition, @inc_Count, @inc_columns,@type_desc, @data_compression_desc, @IndexSizeGB, @user_seeks, @last_user_seek, @user_scans, @last_user_scan, @user_lookups, @Total_User_Escrita,  @Total_User_Leitura, @Dif_Read_Write,
			@allow_row_locks, @row_lock_count, @row_lock_wait_count, @row_lock_wait_in_ms, @allow_page_locks, @page_lock_count, @page_lock_wait_count, @page_lock_wait_in_ms, @fill_factor, @is_padded)
	
		SELECT @inc_columns = null
		
		-- Next index
		fetch ms_crs_ind into @indid, @groupid, @indname, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @auto_created, @no_recompute, @filter_definition,
			@type_desc, @data_compression_desc, @IndexSizeGB, @user_seeks, @last_user_seek, @user_scans, @last_user_scan, @user_lookups, @Total_User_Escrita,  @Total_User_Leitura, @Dif_Read_Write,
			@allow_row_locks, @row_lock_count, @row_lock_wait_count, @row_lock_wait_in_ms, @allow_page_locks, @page_lock_count, @page_lock_wait_count, @page_lock_wait_in_ms, @fill_factor, @is_padded
	end
	deallocate ms_crs_ind

	select
		index_name				
		,index_id				
		,ignore_dup_key			
		,is_unique				
		,is_hypothetical			
		,is_primary_key			
		,is_unique_key			
		,auto_created			
		,no_recompute			
		,groupname				
		,index_keys				
		,filter_definition		
		,inc_Count				
		,inc_columns				
		,[type_desc]				
		,[data_compression_desc] 
		,[IndexSizeGB]			
		,[user_seeks]			
		,[last_user_seek]		
		,[user_scans]			
		,[last_user_scan]		
		,[user_lookups]			
		,[Total_User_Escrita]	
		,[Total_User_Leitura]	
		,[Dif_Read_Write]		
		,[allow_row_locks]		
		,[row_lock_count]		
		,[row_lock_wait_count]	
		,[row_lock_wait_in_ms]	
		,[allow_page_locks]		
		,[page_lock_count]		
		,[page_lock_wait_count]	
		,[page_lock_wait_in_ms]	
		,[fill_factor]			
		,[is_padded]				
	from #tabind
	order by index_id

	return (0)

GO

-- seta a procedure como procedure de sistema para ser usada em qualquer vd
exec sys.sp_MS_marksystemobject 'sp_helpindex_dba'
