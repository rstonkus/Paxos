namespace Paxos

open Paxos
open Paxos.BasicPaxos
open System
open System.Collections.Generic

module Cluster =
  type Config =
    {
      QuorumSize:int
      ProposerCount:int
      RequestTimedOut:int64 -> bool
    }

  let r = System.Random()
  let standardClusterOf3 =
    {
      QuorumSize=2
      ProposerCount=3
      RequestTimedOut = fun t -> DateTime.Now.Ticks - t > (TimeSpan.FromSeconds (float (4+r.Next(2)))).Ticks
    }

  type Node(me:string, config:Config, sendRemote: Destination -> Msg -> unit, response:Value -> unit) =
    let mutable requestInProgress = None
    let mutable pState = freshPState
    let mutable cState = freshCState
    let mutable aState = freshAState
    let mutable lState = freshLState
    let loopbackQueue = Queue<Destination * BasicPaxos.Msg>()
    let sendLocal (d:Destination) (m:BasicPaxos.Msg) =
      loopbackQueue.Enqueue(d,m)
    
    let send (d:Destination, m:Msg) = 
      match d with
      | Proposer s ->
        if s = me
        then sendLocal d m
        else sendRemote d m
      | Broadcast ->
        sendLocal d m
        sendRemote d m
      | BroadcastAcceptors ->
        sendLocal d m
        sendRemote d m
      | External s -> 
        match m with
        | LMsg (MResponse (g,(v,_))) -> 
          if (Option.forall ((=) g) requestInProgress) then requestInProgress <- None
          response v
        | _ -> 
          failwith "Sending a non-response to external. Makes no sense. Bug"
      | Acceptor s ->
        if s = me
        then sendLocal d m
        else sendRemote d m
    
    member this.HasRequestInProgress () =
      requestInProgress.IsSome

    member this.HandleLocalMessage() =
      if(loopbackQueue.Count <> 0)
      then let (d,m) = loopbackQueue.Dequeue()
           in this.Consume me d m

    member this.CheckTimeout () =
      let (cs',ps',o) = clientCheckActiveRequest cState pState DateTime.Now.Ticks config.RequestTimedOut
      pState <- ps'
      cState <- cs'
      o |> Option.iter (wrapP >> send)

    member this.Request (eReq:ExternalRequest) =
        if (requestInProgress.IsSome) then failwith "Only 1 request a time" //TODO Handles only one request a time

        let ((g,_),_) = eReq
        let () = requestInProgress <- Some g
        in this.Consume me (Proposer me) (EMsg (MExternalRequest (me,eReq)))

    member this.Consume sender d msg = 
      let now = System.DateTime.Now.Ticks
      let pReceiveFromE m = 
        let (cs',ps',o) = proposerReceiveFromExternal cState pState now m
        pState <- ps'
        cState <- cs'
        o |> Option.iter (wrapP >> send)
      let aReceiveFromP m =
        let (as',o) = acceptorReceiveFromProposer aState m sender 
        aState <- as'
        o |> Option.iter (wrapA >> send)
      let pReceiveFromA m =
        let (cs',ps',o) = proposerReceiveFromAcceptor config.QuorumSize now cState pState m
        pState <- ps'
        cState <- cs'
        o |> Option.iter (wrapP >> send)
      let lReceiveFromA m =
        let (ls', o) = learnerReceiveFromAcceptor lState m sender config.QuorumSize
        lState <- ls'
        o |> Option.iter (wrapL >> send)
      let aReceiveFromA m =
        let (as',o) = acceptorReceiveFromAcceptor config.ProposerCount config.QuorumSize aState m sender
        aState <- as'
        o |> Option.iter (wrapA >> send)
      match (d,msg) with
      | (Proposer _, (EMsg (MExternalRequest _ as m))) -> pReceiveFromE m
      | (BroadcastAcceptors, (PMsg (MPrepare _ as m))) -> aReceiveFromP m
      | (Proposer _, (AMsg (MPromise _ as m))) -> pReceiveFromA m
      | (BroadcastAcceptors, (PMsg (MAccept _ as m))) -> aReceiveFromP m
      | (Proposer _, (AMsg (MNackPrepare _ as m))) -> pReceiveFromA m
      | (Broadcast, (AMsg (MAccepted _ as m))) -> 
        pReceiveFromA m
        aReceiveFromA m
        lReceiveFromA m
      | (External _, (LMsg (MResponse _ as m))) ->
        failwith "received a response meant for external client"
      | (BroadcastAcceptors, (AMsg (MSyncRequest _ as m))) -> 
        aReceiveFromA m
      | (Acceptor _, (AMsg (MSyncResponse _ as m))) -> 
        aReceiveFromA m
      | _ -> failwithf "bad message destination %A" msg


