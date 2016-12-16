Option Explicit On

Imports System.ServiceProcess
Imports System.Threading
Imports System.Data.SqlClient
Imports System.IO

Public Class CartFileSyncService
    Inherits System.ServiceProcess.ServiceBase

    'this process should be run under the B****** account
    'and it needs access to folders on both nodes

    'db connection paramaters
    Private ThreadCount As Integer = 0 'number of threads spawned
    Private DataObject As ASi.DataAccess.SqlHelper   'common Data object

    Private ConnectionString As String = ""
    Private DBSleepSeconds As Integer = 5
    Private Node As String = ""
    Private Const FILE_ACTION_ADD As String = "ADD"
    Private Const FILE_ACTION_DELETE As String = "DELETE"


    'enumeration for state of the service
    Private Enum ServiceStates
        Shutdown = 0
        Paused = 1
        Running = 2
    End Enum

    Private Enum MessageTypes
        Information = 0
        [Error] = 1
    End Enum

    Private ServiceState As ServiceStates = ServiceStates.Paused

#Region " Component Designer generated code "

    Public Sub New()
        MyBase.New()

        ' This call is required by the Component Designer.
        InitializeComponent()

        ' Add any initialization after the InitializeComponent() call

    End Sub

    'UserService overrides dispose to clean up the component list.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    ' The main entry point for the process
    <MTAThread()> _
    Shared Sub Main()
        Dim ServicesToRun() As System.ServiceProcess.ServiceBase

        ' More than one NT Service may run within the same process. To add
        ' another service to this process, change the following line to
        ' create a second service object. For example,
        '
        '   ServicesToRun = New System.ServiceProcess.ServiceBase () {New Service1, New MySecondUserService}
        '
        ServicesToRun = New System.ServiceProcess.ServiceBase() {New CartFileSyncService}

        System.ServiceProcess.ServiceBase.Run(ServicesToRun)
    End Sub

    'Required by the Component Designer
    Private components As System.ComponentModel.IContainer

    ' NOTE: The following procedure is required by the Component Designer
    ' It can be modified using the Component Designer.  
    ' Do not modify it using the code editor.
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        '
        'CartFileSyncService
        '
        Me.ServiceName = "CartFileSyncService"

    End Sub

#End Region

    Protected Overrides Sub OnStart(ByVal args() As String)
        ' Add code here to start your service. This method should set things
        ' in motion so your service can do its work.

        Try
            'read the basic operating parameters
            ReadAppSettings()

        Catch ex As Exception

            'LogEvent, Send E-mail, and quit
            Dim strMessage As String = "Service is unable to proceed. Shutting down. " & ex.Message
            'log the error
            LogEvent("Service_OnStart", strMessage, MessageTypes.Error)

            'initiate stop process
            InitiateStop()
            Exit Sub
        End Try

        'start an endless loop for service processing the queue
        ThreadPool.QueueUserWorkItem(AddressOf ServiceRun)
    End Sub

    Protected Overrides Sub OnStop()
        ' Add code here to perform any tear-down necessary to stop your service.
        'warn threads we are shutting down
        ServiceState = ServiceStates.Shutdown

        'log the fact that we are "starting to stop"
        LogEvent("OnStop", "Begin OnStop", MessageTypes.Information)

        'give threads up to Delay seconds to wrap things up (note - delay is in milliseconds)
        Dim dtEndWait As Date = Now.AddMilliseconds(5 * 1000)
        While Now <= dtEndWait
            If ThreadCount = 0 Then
                Exit While
            End If
        End While

        'log event that we have stopped
        LogEvent("OnStop", "Service Stopped", MessageTypes.Information)

    End Sub

    Protected Overrides Sub OnShutdown()
        'calls the Windows service OnStop method with the OS shuts down.
        OnStop()
    End Sub

    ''' <summary>
    '''     Programmatically stop the main thread if we've already started up
    '''     such as during the database connection
    ''' </summary>
    Private Sub InitiateStop()
        Dim sc As New ServiceController(Me.ServiceName)
        sc.Stop()
        sc = Nothing
    End Sub
    ''' <summary>
    '''     Common code for writing an event to the event log database of the specified type.
    ''' </summary>
    ''' <param name="Source">source procedure reporting the event.</param>
    ''' <param name="Message">actual event message.</param>
    ''' <param name="MessageType">LogEvent object indicator specifying whether the message is error, informational, start, finish, or debug.</param>
    ''' <remarks>
    ''' </remarks>
    Private Sub LogEvent(ByVal Source As String, _
        ByVal Message As String, _
        ByVal MessageType As MessageTypes)

        'log message
        If MessageType = MessageTypes.Error Then
            EventLog.WriteEntry(Source, Message, EventLogEntryType.Error)
        Else
            EventLog.WriteEntry(Source, Message, EventLogEntryType.Information)
        End If
    End Sub

    ''' <summary>
    '''     Worker thread to process the records.
    ''' </summary>
    ''' <param name="State">New thread callback. State is null</param>
    ''' <remarks>
    '''     This runs in a continuous loop until the service is stopped.
    '''     Multiple threads are spawned, one for each TranType. The thread will sleep when no messages are 
    '''     found or when an error occurs.
    ''' </remarks>
    Private Sub ProcessRecords(ByVal State As Object)

        Try
            
            While ServiceState = ServiceStates.Running
                Dim Records As New DataTable 'table of records needing integration
                Dim r As DataRow

                Try
                    'get the records
                    Records = DataObject.ExecuteDataset(ConnectionString, CommandType.StoredProcedure, "FileSyncSelect", _
                                                        New SqlClient.SqlParameter("@Node", Node)).Tables(0)


                Catch ex As Exception
                    'this could be a permissions or connection problem with the database or some larger issue
                    'regardless, log the event and sleep
                    Me.LogEvent("ProcessRecords", "Error calling FileSyncSelect. " & ex.Message, MessageTypes.Error)
                    GoTo SleepThread
                End Try

                For Each r In Records.Rows

                    Dim strStep As String = ""

                    Try

                        strStep = r("FileAction") & " " & r("FilePath")
                        

                        ' insert integration
                        If r("FileAction") = FILE_ACTION_DELETE Then
                            'delete local file
                            Try
                                File.Delete(r("FilePath"))
                            Catch ex As Exception
                                Me.LogEvent("ProcessRecords", "Error on " & Node & " deleting " & r("FilePath") & ex.Message, MessageTypes.Error)
                            End Try
                            

                        ElseIf r("FileAction") = FILE_ACTION_ADD Then
                            Dim Src As String = GetNetworkPathFromLocal(r("FilePath"))
                            Try
                                File.Copy(Src, r("FilePath"), True)
                            Catch ex As Exception
                                Me.LogEvent("ProcessRecords", "Error on " & Node & " adding " & r("FilePath") & " from " & Src & ". " & ex.Message, MessageTypes.Error)
                            End Try
                        End If

                        'cleanup
                        DataObject.ExecuteNonQuery(ConnectionString, CommandType.StoredProcedure, _
                                                    "FileSyncDelete", _
                                                    New SqlClient.SqlParameter("@Node", Node), _
                                                    New SqlClient.SqlParameter("@FileAction", r("FileAction")), _
                                                    New SqlClient.SqlParameter("@FilePath", r("FilePath")))

                        


                    Catch ex As Exception
                        'error processing record - treat as recoverable but sleep all threads
                        Me.LogEvent("ProcessRecords", "Error on thread " & Node & " while " & strStep & ex.Message, MessageTypes.Error)

                        'stop processing this set of records
                        'we will fall into sleep after we exit the loop
                        Exit For 'exit For Next Each record,
                    End Try 'inner Try to 

                    If ServiceState <> ServiceStates.Running Then
                        'a request to shutdown the service has occured.
                        'bail out of the thread immediately
                        Records = Nothing
                        Exit While
                    End If
                Next 'each record

SleepThread:
                'no (more) records, or an error forced us here - sleep this thread
                Records = Nothing
                Thread.Sleep(DBSleepSeconds * 1000)

            End While 'main loop - ServiceStates.Running

        Catch ex As Exception
            'critical error - this aborts the thread
            Me.LogEvent("ProcessRecords", "Critical error. File actions won't be processed. " & _
            ex.Message, MessageTypes.Error)
        End Try

        'decrement the thread count
        Interlocked.Decrement(ThreadCount)

    End Sub

    Private Function GetNetworkPathFromLocal(ByVal LocalPath As String) As String
        'parse filename and path 
        'accept \images\, \images2\, \pemfiles\ or \pemfiles2\
        'if images, need to add accountid to result path

        Dim Result As String = ""
        Dim BasePath As String = ""
        Dim AccountID As String = ""
        Dim Filename As String = ""
        Dim a() As String = Split(LocalPath, "\")
        If a.Length > 2 Then
            Filename = a(UBound(a)) 'last element in the array
            AccountID = a(UBound(a) - 1) '2nd to last if an image file
        End If
        
        If InStr(LocalPath, "\images\", CompareMethod.Text) > 0 Then
            BasePath = ReadAppSetting("Images")
        ElseIf InStr(LocalPath, "\images2\", CompareMethod.Text) > 0 Then
            BasePath = ReadAppSetting("Images2")
        ElseIf InStr(LocalPath, "\pemfiles\", CompareMethod.Text) > 0 Then
            BasePath = ReadAppSetting("PemFiles")
            'clear accountid
            AccountID = ""
        ElseIf InStr(LocalPath, "\pemfiles2\", CompareMethod.Text) > 0 Then
            BasePath = ReadAppSetting("PemFiles2")
            'clear accountid
            AccountID = ""
        End If

        If BasePath <> "" And Filename <> "" Then
            'get mapped path from local path
            If Right(BasePath, 1) <> "\" Then
                'add the trailing \ if it doesn't exist
                BasePath &= "\"
            End If
            If AccountID <> "" AndAlso Right(AccountID, 1) <> "\" Then
                'add the trailing \ if we have an account id (image file) and it doesn't exist
                AccountID &= "\"
            End If
            'build the result: base\[accountid\]filename to mapped
            Result = BasePath & AccountID & Filename

            'create the local folder if necessary
            Dim FolderName As String = Replace(LocalPath, Filename, "")
            If Right(FolderName, 1) = "\" Then
                FolderName = Left(FolderName, Len(FolderName) - 1)
            End If
            'LogEvent("LocalPath", LocalPath, MessageTypes.Information)
            'LogEvent("Filename", Filename, MessageTypes.Information)
            'LogEvent("Foldername", FolderName, MessageTypes.Information)
            If FolderName <> "" And Directory.Exists(FolderName) = False Then
                'LogEvent("Exists", "False", MessageTypes.Information)
                Directory.CreateDirectory(FolderName)
            End If
            'LogEvent("Exists2", Directory.Exists(FolderName), MessageTypes.Information)

        End If
        Return Result
    End Function
    ''' <summary>
    '''     Retrieve a single parameter from app.config.
    ''' </summary>
    ''' <param name="Key">The name of the key being retrieved.</param>
    Private Function ReadAppSetting(ByVal Key As String) As String

        On Error Resume Next
        Dim AppSettingsReader As New System.Configuration.AppSettingsReader
        Dim strReturnValue As String = ""
        Key = Trim(Key)
        If Key <> "" Then
            'get the value
            strReturnValue = CType(AppSettingsReader.GetValue(Key, GetType(System.String)), String)
        End If
        AppSettingsReader = Nothing
        Return strReturnValue
    End Function
    ''' <summary>
    '''     Reads the basic app.config values.
    ''' </summary>
    Private Sub ReadAppSettings()
        'Purpose:   Read the basic app.config settings

        'get DB connect string key
        ConnectionString = ReadAppSetting("ConnectionString") 'get connect string key

        'get DB connect delay
        If IsNumeric(ReadAppSetting("DBSleep")) AndAlso _
            CType(ReadAppSetting("DBSleep"), Integer) > 0 Then
            DBSleepSeconds = CType(ReadAppSetting("DBSleep"), Integer)
        End If

        'get Node
        Node = ReadAppSetting("NodeID")

    End Sub

    ''' <summary>
    '''     Main thread for the service.
    ''' </summary>
    ''' <param name="State">New thread callback.</param>
    ''' <remarks>
    '''     This runs in a continuous loop until the service is stopped.
    '''     Multiple threads are spawned for each message in the queue, up to the 
    '''     ConfigValue.MaxThreads value. The thread will sleep when no messages are 
    '''     found in the queue or when a recoverable error occurs.
    ''' </remarks>
    Protected Sub ServiceRun(ByVal State As Object)

        Try

            'set our status to run mode
            ServiceState = ServiceStates.Running

            'increment the thread count (each thread will decrement this when its done)
            Interlocked.Increment(ThreadCount)

            'process records on a separate thread that we can sleep
            ThreadPool.QueueUserWorkItem(AddressOf ProcessRecords)

        Catch ex As Exception

            Dim strMessage As String = "Service is unable to proceed. Shutting down. " & ex.Message
            'log the error
            LogEvent("Service_OnStart", strMessage, MessageTypes.Error)

            'initiate stop process

            Dim sc As New ServiceController(Me.ServiceName)
            sc.Stop()
            Exit Sub
        End Try
    End Sub
End Class
