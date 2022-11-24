/*

CB: Marek
Desc: Podsumowanie ciekawego w¹tku dotycz¹cego asynychronicznego wykonywania procedur w Sql Server.
- Mój wk³ad: t³umaczenie (swobodne, wiêkszoœæ niewiele wnosi) i zebranie kodu do jednego pliku. Kod nieco zmodyfikowany, ¿eby sprawnie przejœæ do wyniku usun¹³em transakcje.
- Wykonuje siê trochê czasu (do 30s) ze wzglêdu na waitfory. Nie ma tu nic obci¹¿aj¹cego.
- Pewnym u³atwieniem jest skrypt po klikniêciu PPM na "Service Broker" w danej db, otrzymujemy zestaw instrukcji koniecznych do zbudowania nowej "aplikacji SB".
- Ca³oœæ w tym przypadku mo¿e wygl¹daæ na bardziej skomplikowan¹, ni¿ w rzeczywistoœci jest, W ró¿nych artyku³ach widaæ mniejsze lub wiêksze komplikacje.

- W wyniku kluczowe jest sprawdzenie submit_time/start_time/finish_time

Materia³:

Oryginalny w¹tek SO:
https://stackoverflow.com/questions/1229438/execute-a-stored-procedure-from-a-windows-form-asynchronously-and-then-disconnect

Oryginalny artyku³:
http://rusanu.com/2009/08/05/asynchronous-procedure-execution/

Dokumentacja MS:
https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/sql-server-service-broker?view=sql-server-ver16

Opis dzia³ania po polsku:
http://wsinf.edu.pl/assets/img/pdf/Zeszyty%20naukowe/vol.10%20nr%202/528.pdf

Inne ciekawe po polsku:
https://www.mobilo24.eu/dzialajacy-przyklad-service-brokera-ms-sql-2008/

Koniecznie baza musi mieæ uruchomiony SB, np:

--USE AdventureWorks2012
--GO

--ALTER DATABASE AdventureWorks2012 SET SINGLE_USER WITH ROLLBACK IMMEDIATE
--ALTER DATABASE AdventureWorks2012 SET ENABLE_BROKER
--ALTER DATABASE AdventureWorks2012 SET MULTI_USER
--GO

sprawdzamy to:
SELECT name, is_broker_enabled FROM sys.databases

Wiêcej o troubleshooting SB:
https://learn.microsoft.com/en-us/troubleshoot/system-center/scom/troubleshoot-sql-server-service-broker-issues

*/

-- dropy ¿eby mo¿na by³o zawsze wykonaæ
drop table if exists [AsyncExecResults];
go

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'dbo.[AsyncExecQueue]')
) 
begin
drop service [AsyncExecService];
drop queue [AsyncExecQueue];
end

go


/*
Na pocz¹tek tworzymy tabelê, która pos³u¿y prezentacji dzia³ania SB.
*/

--1. Tabela z danymi przekazanych procedur.

create table [AsyncExecResults] (
	[token] uniqueidentifier primary key
	, [submit_time] datetime not null
	, [start_time] datetime null
	, [finish_time] datetime null
	, [error_number]	int null
	, [error_message] nvarchar(2048) null);

go

/*
Nastêpnie tworzymy service i kolejkê.
*/

--2. Service i queue

create queue [AsyncExecQueue];
go

create service [AsyncExecService] on queue [AsyncExecQueue] ([DEFAULT]);
go

/*
Nastêpnie core æwiczenia: procedura wywo³ywana po przekazaniu innej procedury do SB.
*/

--3. Procedura przyjmuj¹ca aktywowane procedury
create or alter procedure usp_AsyncExecActivated
as
begin
    set nocount on;
    declare @h uniqueidentifier
        , @messageTypeName sysname
        , @messageBody varbinary(max)
        , @xmlBody xml
        , @procedureName sysname
        , @startTime datetime
        , @finishTime datetime
        , @execErrorNumber int
        , @execErrorMessage nvarchar(2048)
        , @xactState smallint
        , @token uniqueidentifier;

    begin try;
        receive top(1) 
            @h = [conversation_handle]
            , @messageTypeName = [message_type_name]
            , @messageBody = [message_body]
            from [AsyncExecQueue];

        if (@h is not null)
        begin
            if (@messageTypeName = N'DEFAULT')
            begin
                -- The DEFAULT message type is a procedure invocation.
                -- Extract the name of the procedure from the message body.
                --
                select @xmlBody = CAST(@messageBody as xml);
                select @procedureName = @xmlBody.value(
                    '(//procedure/name)[1]'
                    , 'sysname');

                select @startTime = GETUTCDATE();
                begin try

                    exec @procedureName;

                end try
                begin catch

					select @execErrorNumber = ERROR_NUMBER(),
						@execErrorMessage = ERROR_MESSAGE(),
						@xactState = XACT_STATE();

					if (@xactState = -1)
					begin

						raiserror(N'Unrecoverable error in procedure %s: %i: %s', 16, 10,
							@procedureName, @execErrorNumber, @execErrorMessage);
					end
					else if (@xactState = 1)
					begin
						print 1;
					end

                end catch

                select @finishTime = GETUTCDATE();
                select @token = [conversation_id] 
                    from sys.conversation_endpoints 
                    where [conversation_handle] = @h;
                if (@token is null)
                begin
                    raiserror(N'Internal consistency error: conversation not found', 16, 20);
                end
                update [AsyncExecResults] set
                    [start_time] = @starttime
                    , [finish_time] = @finishTime
                    , [error_number] = @execErrorNumber
                    , [error_message] = @execErrorMessage
                    where [token] = @token;
                if (0 = @@ROWCOUNT)
                begin
                    raiserror(N'Internal consistency error: token not found', 16, 30);
                end
                end conversation @h;
            end 
            else if (@messageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog')
            begin
                end conversation @h;
            end
            else if (@messageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/Error')
            begin
                declare @errorNumber int
                    , @errorMessage nvarchar(4000);
                select @xmlBody = CAST(@messageBody as xml);
                with xmlnamespaces (DEFAULT N'http://schemas.microsoft.com/SQL/ServiceBroker/Error')
                select @errorNumber = @xmlBody.value ('(/Error/Code)[1]', 'INT'),
                    @errorMessage = @xmlBody.value ('(/Error/Description)[1]', 'NVARCHAR(4000)');
                -- Update the request with the received error
                select @token = [conversation_id] 
                    from sys.conversation_endpoints 
                    where [conversation_handle] = @h;
                update [AsyncExecResults] set
                    [error_number] = @errorNumber
                    , [error_message] = @errorMessage
                    where [token] = @token;
                end conversation @h;
             end
           else
           begin
                raiserror(N'Received unexpected message type: %s', 16, 50, @messageTypeName);
           end
        end

    end try
    begin catch
        declare @error int
            , @message nvarchar(2048);
        select @error = ERROR_NUMBER()
            , @message = ERROR_MESSAGE()
            , @xactState = XACT_STATE();
        if (@xactState <> 0)
        begin
			print 1;
        end;
        raiserror(N'Error: %i, %s', 1, 60,  @error, @message) with log;
    end catch
end;

go

/*
Aby aktywowaæ procedurê nale¿y "przyczepiæ" j¹ do kolejki. W ten sposób wykona siê z ka¿dym wywo³aniem.

Tutaj pewnie brakuj¹cym elementem jest przekazanie parametrów ( http://rusanu.com/2009/08/18/passing-parameters-to-a-background-procedure/ ).

Max_queue_readers to prawdopodobnie maksymalna iloœæ dedykowanych w¹tków. Temat skalowalnoœci jest szeroko poruszony w artykule https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008/dd576261(v=sql.100)?redirectedfrom=MSDN

Nie ma jasnej odpowiedzi, jaka iloœæ jest optymalna. W w¹tku https://www.sqlservercentral.com/forums/topic/recommendations-for-max_queue_reader-option-on-queue
ktoœ s³usznie zwraca uwagê, ¿e trzeba to testowaæ samemu, poniewa¿ wszystkie teoretyczne testy skupiaj¹ siê na prostych zadaniach maj¹cych wykazaæ zmiany w performance samego SB. Nie uwzglêdniaj¹ skomplikowania samej procedury, która sama w sobie mo¿e byæ przecie¿ bardzo ciê¿ka.

W przeciwieñstwie do zwyk³ych jobów, SB ma teoretycznie swoje mechanizmy reguluj¹ce ob³o¿enie w¹tku.
*/

--4. "Przyczepienie" procedury do kolejki.

alter queue [AsyncExecQueue]
    with activation (
    procedure_name = [usp_AsyncExecActivated]
    , max_queue_readers = 10 --zmieniam 1 na 10, tylko na potrzebê æwiczenia
    , execute as owner
    , status = on);

go

/*
Ostatnim elementem uk³adanki jest procedura, która przekazuje sygna³ wywo³uj¹cy porz¹dan¹ procedurê asynchroniczn¹.
*/

--5. Procedura wywo³uj¹ca asynchroniczne procedury przyczepione do kolejki.

create or alter procedure [usp_AsyncExecInvoke]
    @procedureName sysname
    , @token uniqueidentifier output
as
begin
    declare @h uniqueidentifier
	    , @xmlBody xml
        , @trancount int;
    set nocount on;

	set @trancount = @@trancount;

    begin try
        begin dialog conversation @h
            from service [AsyncExecService]
            to service N'AsyncExecService', 'current database'
            with encryption = off;
        select @token = [conversation_id]
            from sys.conversation_endpoints
            where [conversation_handle] = @h;
        select @xmlBody = (
            select @procedureName as [name]
            for xml path('procedure'), type);
        send on conversation @h (@xmlBody);
        insert into [AsyncExecResults]
            ([token], [submit_time])
            values
            (@token, getutcdate());

    end try
    begin catch
        declare @error int
            , @message nvarchar(2048)
            , @xactState smallint;
        select @error = ERROR_NUMBER()
            , @message = ERROR_MESSAGE()
            , @xactState = XACT_STATE();

        raiserror(N'Error: %i, %s', 16, 1, @error, @message);
    end catch
end

go

/*
Aby przetestowaæ nasz¹ "asynchroniczn¹ infrastrukturê" tworzymy testow¹ procedurê i wywo³ujemy j¹ asynchronicznie.

W tym celu tworzymy dwie procedury - jedn¹ trwaj¹c¹ 5 sekund maj¹c¹ udawaæ d³ugo wykonuj¹c¹ siê sp, i drug¹, która celowo koñczy siê b³êdem i ma za zadanie pokazaæ, jak zachowa siê mechanizm w przypadku wyst¹pienia problemów.

Od siebie dorzuci³em trzeci¹, bardzo krótk¹.

*/

--6. Dwie przyk³adowe procedury do przetestowania mechanizmu.

create or alter procedure [usp_MyLongRunningProcedure]
as
begin
    waitfor delay '00:00:10';
end
go

create or alter procedure [usp_MyShortAndEzProc]
as
begin
    select 1;
end
go

create or alter procedure [usp_MyFaultyProcedure]
as
begin
    set nocount on;
    declare @t table (id int primary key);
    insert into @t (id) values (1);
    insert into @t (id) values (1);
end;

go

--7. Przekazanie procedur do kolejki i wywo³anie wyników.
-- zakomentowa³em selecty, ¿eby nie rzucaæ tyle wyników

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyLongRunningProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyLongRunningProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyLongRunningProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyLongRunningProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyLongRunningProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyLongRunningProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyFaultyProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyFaultyProcedure', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyShortAndEzProc', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go

declare @token uniqueidentifier;
exec usp_AsyncExecInvoke N'usp_MyShortAndEzProc', @token output;
--select * from [AsyncExecResults] where [token] = @token;
go


-- to jest tylko oczekiwanie na wynik, ¿eby wszystko siê skoñczy³o
waitfor delay '00:00:30';
select * from [AsyncExecResults]
order by submit_time desc;
go


/*
Je¿eli sprawdzimy start time drugiej procedury, widaæ, ¿e rozpoczê³a siê zaraz po pierwszej. Wynika to z deklaracji max_queue_readers=1 (id¹æ za artyku³em, w tym przypadku jest ustawione na 10).

To ogranicza liczbê aktywowanych procedur do maks jednej.

W kolejce nale¿y te¿ zwróciæ uwagê na ustawienie "execute as" czyli po prostu ustawienie u¿ytkownika wykonuj¹cego sp.

*/


