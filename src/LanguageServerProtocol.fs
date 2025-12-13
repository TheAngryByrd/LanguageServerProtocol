namespace Ionide.LanguageServerProtocol

module Server =
  open System
  open System.IO
  open Ionide.LanguageServerProtocol.Logging
  open Ionide.LanguageServerProtocol.Types
  open System.Threading
  open System.Threading.Tasks
  open System.Reflection
  open StreamJsonRpc
  open Newtonsoft.Json
  open Ionide.LanguageServerProtocol.JsonUtils
  open Newtonsoft.Json.Linq
  open StreamJsonRpc
  open StreamJsonRpc.Protocol
  open JsonRpc
  open Mappings

  let logger = LogProvider.getLoggerByName "LSP Server"

  let defaultJsonRpcFormatter () =
    let jsonRpcFormatter = new JsonMessageFormatter()
    jsonRpcFormatter.JsonSerializer.NullValueHandling <- NullValueHandling.Ignore
    jsonRpcFormatter.JsonSerializer.ConstructorHandling <- ConstructorHandling.AllowNonPublicDefaultConstructor
    jsonRpcFormatter.JsonSerializer.MissingMemberHandling <- MissingMemberHandling.Ignore
    jsonRpcFormatter.JsonSerializer.Converters.Add(StrictNumberConverter())
    jsonRpcFormatter.JsonSerializer.Converters.Add(StrictStringConverter())
    jsonRpcFormatter.JsonSerializer.Converters.Add(StrictBoolConverter())
    jsonRpcFormatter.JsonSerializer.Converters.Add(SingleCaseUnionConverter())
    jsonRpcFormatter.JsonSerializer.Converters.Add(OptionConverter())
    jsonRpcFormatter.JsonSerializer.Converters.Add(ErasedUnionConverter())
    jsonRpcFormatter.JsonSerializer.ContractResolver <- OptionAndCamelCasePropertyNamesContractResolver()
    jsonRpcFormatter

  let jsonRpcFormatter = defaultJsonRpcFormatter ()

  let deserialize<'t> (token: JToken) = token.ToObject<'t>(jsonRpcFormatter.JsonSerializer)
  let serialize<'t> (o: 't) = JToken.FromObject(o, jsonRpcFormatter.JsonSerializer)

  let requestHandling<'param, 'result> (run: 'param -> AsyncLspResult<'result>) : Delegate =
    let runAsTask param ct =
      // Execute non-async portion of `run` before forking the async portion into a task.
      // This is needed to avoid reordering of messages from a client.
      let asyncLspResult = run param

      let asyncContinuation =
        async {
          let! lspResult = asyncLspResult

          return
            match lspResult with
            | Ok result -> result
            | Error error ->
              let rpcException = LocalRpcException(error.Message)
              rpcException.ErrorCode <- error.Code

              rpcException.ErrorData <-
                error.Data
                |> Option.defaultValue null

              raise rpcException
        }

      Async.StartAsTask(asyncContinuation, cancellationToken = ct)

    Func<'param, CancellationToken, Task<'result>>(runAsTask) :> Delegate

  /// Notifications don't generate a response or error, but to unify things we consider them as always successful.
  /// They will still not send any response because their ID is null.
  let private notificationSuccess (response: Async<unit>) =
    async {
      do! response
      return Result.Ok()
    }

  type ClientNotificationSender = string -> obj -> AsyncLspResult<unit>

  type ClientRequestSender =
    abstract member Send<'a> : string -> obj -> AsyncLspResult<'a>

  type private MessageHandlingResult =
    | Normal
    | WasExit
    | WasShutdown

  type LspCloseReason =
    | RequestedByClient = 0
    | ErrorExitWithoutShutdown = 1
    | ErrorStreamClosed = 2

  let (|Flatten|) (ex: Exception) : Exception =
    match ex with
    | :? AggregateException as aex ->
      let aex = aex.Flatten()

      if aex.InnerExceptions.Count = 1 then
        aex.InnerException
      else
        aex
    | _ -> ex


  /// The default RPC logic shipped with this library. All this does is mark LocalRpcExceptions as non-fatal
  let defaultRpc (handler: IJsonRpcMessageHandler) =
    { new JsonRpc(handler) with
        member this.IsFatalException(ex: Exception) =
          match ex with
          | Flatten(:? LocalRpcException | :? JsonSerializationException) -> false
          | _ -> true

        member this.CreateErrorDetails(request: JsonRpcRequest, ex: Exception) =
          let isSerializable = this.ExceptionStrategy = ExceptionProcessing.ISerializable

          match ex with
          | Flatten(:? JsonSerializationException as ex) ->
            let data: obj = if isSerializable then ex else CommonErrorData(ex)
            JsonRpcError.ErrorDetail(Code = JsonRpcErrorCode.ParseError, Message = ex.Message, Data = data)
          | _ -> ``base``.CreateErrorDetails(request, ex)
    }

  let startWithSetupCore<'client when 'client :> Ionide.LanguageServerProtocol.ILspClient>
    (setupRequestHandlings: 'client -> Map<string, Delegate>)
    (jsonRpcHandler: IJsonRpcMessageHandler)
    (clientCreator: (ClientNotificationSender * ClientRequestSender) -> 'client)
    (customizeRpc: IJsonRpcMessageHandler -> JsonRpc)
    =
    // Without overriding isFatalException, JsonRpc serializes exceptions and sends them to the client.
    // This is particularly bad for notifications such as textDocument/didChange which don't require a response,
    // and thus any exception that happens during e.g. text sync gets swallowed.
    use jsonRpc = customizeRpc jsonRpcHandler

    /// When the server wants to send a notification to the client
    let sendServerNotification (rpcMethod: string) (notificationObj: obj) : AsyncLspResult<unit> =
      async {
        do!
          jsonRpc.NotifyWithParameterObjectAsync(rpcMethod, notificationObj)
          |> Async.AwaitTask

        return
          ()
          |> LspResult.success
      }

    /// When the server wants to send a request to the client
    let sendServerRequest (rpcMethod: string) (requestObj: obj) : AsyncLspResult<'response> =
      async {
        let! response =
          jsonRpc.InvokeWithParameterObjectAsync<'response>(rpcMethod, requestObj)
          |> Async.AwaitTask

        return
          response
          |> LspResult.success
      }

    let lspClient =
      clientCreator (
        sendServerNotification,
        { new ClientRequestSender with
            member __.Send x t = sendServerRequest x t
        }
      )

    // Note on server shutdown.
    // According the the LSP spec the shutdown sequence consists fo a client sending onShutdown request followed by
    // onExit notification. The server can terminate after receiving onExit. However, real language clients implements
    // the shutdown in their own way:
    // 1. VSCode Language Client has a bug that causes it to NOT send an `exit` notification when stopping a server:
    //    https://github.com/microsoft/vscode-languageserver-node/pull/776
    //    VSCode sends onShutdown and then closes the connection.
    // 2. Neovim LSP sends onShutdown followed by onExit but does NOT close the connection on its own.
    // 3. Emacs LSP mode sends onShutdown followed by onExit and then closes the connection.
    // This is the reason for the complicated logic below.

    let mutable shutdownReceived = false
    let mutable quitReceived = false
    use quitSemaphore = new SemaphoreSlim(0, 1)

    let onShutdown () =
      logger.trace (Log.setMessage "Shutdown received")
      shutdownReceived <- true

    jsonRpc.AddLocalRpcMethod("shutdown", Action(onShutdown))

    let onExit () =
      logger.trace (Log.setMessage "Exit received")
      quitReceived <- true

      quitSemaphore.Release()
      |> ignore

    jsonRpc.AddLocalRpcMethod("exit", Action(onExit))

    for handling in setupRequestHandlings lspClient do
      let rpcMethodName = handling.Key
      let rpcDelegate = handling.Value

      let rpcAttribute = JsonRpcMethodAttribute(rpcMethodName)
      rpcAttribute.UseSingleObjectParameterDeserialization <- true

      jsonRpc.AddLocalRpcMethod(rpcDelegate.GetMethodInfo(), rpcDelegate.Target, rpcAttribute)

    jsonRpc.StartListening()

    // 1. jsonRpc.Completion finishes when either a connection is closed or a fatal exception is thrown.
    // 2. quitSemaphore is released when the server receives both onShutdown and onExit.
    // Completion of either of those causes the server to stop.
    let completed_task_idx = Task.WaitAny(jsonRpc.Completion, quitSemaphore.WaitAsync())
    // jsonRpc.Completion throws on fatal exception. However, Task.WaitAny doesn't even when jsonRpc.Completion would.
    // Here we check and re-raise if needed.
    if completed_task_idx = 0 then
      match jsonRpc.Completion.Exception with
      | null -> ()
      | exn -> raise exn

    match shutdownReceived, quitReceived with
    | true, true -> LspCloseReason.RequestedByClient
    | false, true -> LspCloseReason.ErrorExitWithoutShutdown
    | _ -> LspCloseReason.ErrorStreamClosed

  let startWithSetup<'client when 'client :> Ionide.LanguageServerProtocol.ILspClient>
    (setupRequestHandlings: 'client -> Map<string, Delegate>)
    (input: Stream)
    (output: Stream)
    (clientCreator: (ClientNotificationSender * ClientRequestSender) -> 'client)
    (customizeRpc: IJsonRpcMessageHandler -> JsonRpc)
    =
    use jsonRpcHandler = new HeaderDelimitedMessageHandler(output, input, defaultJsonRpcFormatter ())
    startWithSetupCore setupRequestHandlings jsonRpcHandler clientCreator customizeRpc

  let startWithSetupWs<'client when 'client :> Ionide.LanguageServerProtocol.ILspClient>
    (setupRequestHandlings: 'client -> Map<string, Delegate>)
    (socket: System.Net.WebSockets.WebSocket)
    (clientCreator: (ClientNotificationSender * ClientRequestSender) -> 'client)
    (customizeRpc: IJsonRpcMessageHandler -> JsonRpc)
    =
    use jsonRpcHandler = new WebSocketMessageHandler(socket, defaultJsonRpcFormatter ())
    startWithSetupCore setupRequestHandlings jsonRpcHandler clientCreator customizeRpc


  let serverRequestHandling<'server, 'param, 'result when 'server :> Ionide.LanguageServerProtocol.ILspServer>
    (run: 'server -> 'param -> AsyncLspResult<'result>)
    : ServerRequestHandling<'server> =
    { Run = fun s -> requestHandling (run s) }

  let defaultRequestHandlings () : Map<string, ServerRequestHandling<'server>> =
    routeMappings ()
    |> Map.ofList

  let private requestHandlingSetupFunc<'client, 'server
    when 'client :> Ionide.LanguageServerProtocol.ILspClient and 'server :> Ionide.LanguageServerProtocol.ILspServer>
    (requestHandlings: Map<string, ServerRequestHandling<'server>>)
    (serverCreator: 'client -> 'server)
    =
    fun client ->
      let server = serverCreator client

      requestHandlings
      |> Map.map (fun _ requestHandling -> requestHandling.Run server)

  let start<'client, 'server
    when 'client :> Ionide.LanguageServerProtocol.ILspClient and 'server :> Ionide.LanguageServerProtocol.ILspServer>
    (requestHandlings: Map<string, ServerRequestHandling<'server>>)
    (input: Stream)
    (output: Stream)
    (clientCreator: (ClientNotificationSender * ClientRequestSender) -> 'client)
    (serverCreator: 'client -> 'server)
    =
    let requestHandlingSetup = requestHandlingSetupFunc requestHandlings serverCreator
    startWithSetup requestHandlingSetup input output clientCreator

  let startWs<'client, 'server
    when 'client :> Ionide.LanguageServerProtocol.ILspClient and 'server :> Ionide.LanguageServerProtocol.ILspServer>
    (requestHandlings: Map<string, ServerRequestHandling<'server>>)
    (socket: Net.WebSockets.WebSocket)
    (clientCreator: (ClientNotificationSender * ClientRequestSender) -> 'client)
    (serverCreator: 'client -> 'server)
    =
    let requestHandlingSetup = requestHandlingSetupFunc requestHandlings serverCreator
    startWithSetupWs requestHandlingSetup socket clientCreator