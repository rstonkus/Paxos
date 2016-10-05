namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open System.Collections.Generic



module Format =

  let pState (s:PState) =
    match s with
    | PReady n -> sprintf "PReady %i" n
    | PPrepareSent (n,cr,vvos) -> sprintf "PPrepareSent %i" n
    | PAcceptSent (n,cr,vvs) -> sprintf "PPrepareSent %i" n

  let aState (AReady (n,store):AState) =
    sprintf "AReady (%i, %A)" n store

  let lState (LReady store) =
    sprintf "%A" store

  let pMsg (m:PMsg) = 
    match m with
    | MPrepare (n,k) -> sprintf "MPrepare (%i,%s)" n k
    | MAccept (n,_,k,v) -> sprintf "MAccept (%i,%s,%A)" n k v

  let aMsg (m:AMsg) =
    match m with
    | MPromise (n,vvo) -> sprintf "MPromise (%i,%A)" n vvo
    | MAccepted (n,_,k,v) -> sprintf "MAccepted (%i,%s,%A)" n k v
    | MNackPrepare (n,n') -> sprintf "MNackPrepare (%i,%i)" n n'

  let lMsg (m:LMsg) =
    match m with
    | MResponse (g,v) -> sprintf "MResponse (%s,%A)" (g.ToString ()) v

  let cMsg (m:CMsg) =
    match m with
    | MClientRequest ((g,s),k,f) -> sprintf "MClientRequest (%s,%s,%s)" (g.ToString()) s k

  let msg (m:Msg) =
    match m with
    | AMsg x -> aMsg x
    | PMsg x -> pMsg x
    | LMsg x -> lMsg x
    | CMsg x -> cMsg x

  let dest (d:Destination) =
    sprintf "%A" d

  let destMsg ((d,m):(Destination * Msg)) :string= 
    sprintf "(%s,%s)" (dest d) (msg m)

  let option f o =
    match o with
    | None -> "None"
    | Some x -> sprintf "Some %s" (f x)

  let participant (p:Participant.Participant) =  
    let crashed c = (if c <> 0 then sprintf "XX%iXX " c else "")
    match p with
    | Participant.Proposer x -> sprintf "%s: inreq=%i, in=%i, out=%i, %s" x.Name x.InputRequests.Count x.Input.Count x.Output.Count (pState x.PState)
    | Participant.Acceptor x -> sprintf "%s: %sin=%i, out=%i, %s" x.Name (crashed x.CrashedFor) x.Input.Count x.Output.Count (aState x.AState)
    | Participant.Client x -> sprintf "%s: in=%i, out=%i" x.Name x.Input.Count x.Output.Count
    | Participant.Learner x -> sprintf "%s: in=%i, out=%i, %s" x.Name x.Input.Count x.Output.Count (lState x.LState)

  let participants ps =
    System.String.Join ("\n", ps |> Seq.map participant)

module Run =
  type Result =
    {
      mutable Responses : (string * Sender * Msg) list 
    }

  module Verify =
    let responsesToGuid g result = 
      let f (_,_,m) =
        match m with
        | LMsg (MResponse (guid,v)) -> 
          if (g = guid)
          then Some v
          else None
        | _ -> failwith "a msg from a non-learner sent the result"
      result.Responses |> Seq.choose f
  
    let responsesToClient name result =
      result.Responses |> Seq.filter (fun (n,_,_) -> n = name)


  let clientSend (client:Participant.Client) m =
    client.Output.Enqueue m

  //puts the msg in the right input queue
  let sendMsg participants sender m = 
    let send msg d = Participant.sendToDest (sender,msg) d participants
    let broadcast msg = Participant.broadcast participants (sender,msg)
    let broadcastA msg = Participant.broadcastA participants (sender,msg)
    match m with
    | (Proposer d, (CMsg (MClientRequest _) as msg)) -> send msg d
    | (BroadcastAcceptors, (PMsg (MPrepare _) as msg)) -> broadcastA msg
    | (Proposer d, (AMsg (MPromise _) as msg)) -> send msg d
    | (BroadcastAcceptors, (PMsg (MAccept _) as msg)) -> broadcastA msg
    | (Proposer d, (AMsg (MNackPrepare _) as msg)) -> send msg d
    | (Broadcast, (AMsg (MAccepted _) as msg)) -> broadcast msg
    | (Client d, (LMsg (MResponse _) as msg)) -> send msg d
    | _ -> failwithf "bad message destination %A" m
  
  let wrapA (d,m) = (d,AMsg m)
  let wrapP (d,m) = (d,PMsg m)
  let wrapL (d,m) = (d,LMsg m)
  let wrapC (d,m) = (d,CMsg m)
  
  let consumeRequestIfReady participants (p:Participant.Proposer) : bool =
    let s = p.PState
    match s with
    | PReady _ ->
      let (s',outMsgO) = proposerReceiveFromClient s (p.InputRequests.Dequeue ())
      let () = 
        outMsgO 
        |> Option.map wrapP 
        |> Option.iter (sendMsg participants p.Name)
      let () =p.PState <- s'
      true
    | _ -> 
      false



  let consumeMsg debug quorumSize participants result (p:Participant.Participant) =
    match p with
    | Participant.Acceptor a ->
      let (sender, msg) = a.Input.Dequeue ()
      let printDebug s' outDestMsgO = 
        if debug 
        then printfn "%s <- (%s,%s) --> %s out: %s" a.Name sender (Format.msg msg) (Format.aState s') (Format.option Format.destMsg outDestMsgO)
        else ()
      match msg with
      | PMsg pmsg -> let (s',outMsgO) = acceptorReceiveFromProposer a.AState pmsg sender
                     do let outDestMsgO = outMsgO |> Option.map wrapA
                        printDebug s' outDestMsgO
                        Option.iter a.Output.Enqueue outDestMsgO
                        a.AState <- s'
      | AMsg amsg -> let (s',outMsgO) = acceptorReceiveFromAcceptor a.AState amsg sender
                     do let outDestMsgO = outMsgO |> Option.map wrapA
                        printDebug s' outDestMsgO
                        Option.iter a.Output.Enqueue outDestMsgO
                        a.AState <- s'
      | _ -> failwithf "Acceptor received unexpected message %A" msg
    | Participant.Proposer p ->
      let (sender, msg) = p.Input.Dequeue ()
      let printDebug s' outDestMsgO = 
        if debug 
        then printfn "%s <- (%s,%s) --> %s out: %s" p.Name sender (Format.msg msg) (Format.pState s') (Format.option Format.destMsg outDestMsgO)

      match msg with
      | CMsg cmsg -> p.InputRequests.Enqueue cmsg
      | AMsg amsg -> let (s',outMsgO) = proposerReceiveFromAcceptor quorumSize p.PState amsg
                     do let outDestMsgO = outMsgO |> Option.map wrapP
                        printDebug s' outDestMsgO
                        Option.iter p.Output.Enqueue outDestMsgO
                        p.PState <- s'
      | _ -> failwithf "Proposer received unexpected message %A" msg
    | Participant.Learner l ->
      let (sender, msg) = l.Input.Dequeue ()
      let r = 
        match msg with
        | AMsg amsg -> let (s', outMsgO) = learnerReceiveFromAcceptor l.LState amsg sender quorumSize
                       let outDestMsgO = outMsgO |> Option.map wrapL
                       if debug then printfn "%s <- (%s,%s) out: %s" l.Name sender (Format.msg msg) (Format.option Format.destMsg outDestMsgO)
                       Option.iter l.Output.Enqueue outDestMsgO
                       l.LState <- s'
        | _ -> failwithf "Learner received unexpected message %A" msg
      do //if debug then printfn "--> %A" l.LState
         r
    | Participant.Client c ->
      let (sender, msg) = c.Input.Dequeue ()
      result.Responses <- (c.Name, sender, msg) :: result.Responses

  let unfinishedParticipants ps =
    let canSend =
      ps
      |> Seq.filter (fun p -> (Participant.output p).Count <> 0)
      |> Seq.filter (Participant.isCrashed >> not)
      |> Seq.toArray
    let canHandle =
      ps
      |> Seq.filter (Participant.isCrashed >> not)
      |> Seq.filter (fun p -> (Participant.input p).Count <> 0)
      |> Seq.toArray
    let reqsWaiting =
      ps
      |> Seq.choose Participant.tryProposer
      |> Seq.filter (fun p -> p.InputRequests.Count <> 0)
      |> Seq.toArray

    in (canHandle,canSend,reqsWaiting)

  let runToEnd (debug:bool) (random:System.Random) emulateMessageLosses quorumSize participants result = 
    let pickRandom xs =
      let length = Array.length xs
      let i = random.Next length
      in xs.[i]
    let consume = consumeMsg debug quorumSize participants result
    let send = sendMsg participants
    let mutable unfinished = unfinishedParticipants participants //not counting client inputs
    let mutable c = true
    let mutable step = 0
    while c
      do
        //printfn "-------------------------\n%A" participants
        
        let (canHandle,canSend,reqsWaiting) = unfinished
        match random.Next 5 with
        |0|1|2|3 -> //progress
          if debug then printfn "-------------- %i --------------\n%s" step (Format.participants participants)
          match random.Next 6 with
          //consume message -- 1 message consumed can result in 4 messages sent (broadcast -- assuming cluster size 3)
          | 0 | 1 | 2 -> 
            if (Array.isEmpty canHandle)
            then if debug then printfn "nop - nothing to handle" else ()
            else let participant = pickRandom canHandle
                 consume participant
          //send message
          | 3 | 4 -> 
            if (Array.isEmpty canSend)
            then if debug then printfn "nop - nothing to send" else ()
            else let participant = pickRandom canSend
                 let name = Participant.name participant
                 let outBuf = Participant.output participant
                 let (d,m) = (outBuf.Dequeue ())
                 if debug then printfn "Send msg: %s, %s" (Participant.name participant) (Format.msg m)
                 send name (d,m)
          | _ -> 
            if (reqsWaiting.Length <> 0)
            then let p = pickRandom reqsWaiting
                 if consumeRequestIfReady participants p
                 then if debug then printfn "%s: client request" p.Name else ()
                 else if debug then printfn "nop - proposer not ready to consume request" else ()
            else if debug then printfn "nop - no requests waiting for proposer" else ()
          
        | _ -> //maybe do something evil
          if (random.Next 50 <> 0)
          then () // P(evil) = 1/5 * 1/20 = 1/100
          else let all =  Participant.allA participants
               let alive = all |> Seq.filter (fun a -> a.CrashedFor = 0) |> Seq.toArray
               if(Seq.length alive <= quorumSize) 
               then if debug then printfn "nop - too many crashed" else () //we cannot crash more according to assumptions
               else let toCrash = pickRandom alive
                    let rounds = (random.Next 100) + 10 //at least 10 rounds
                    if debug then printfn "XXX Crashing %s for %i rounds" toCrash.Name rounds
                    if emulateMessageLosses then toCrash.Input.Clear()
                    toCrash.CrashedFor <- rounds
                    if (random.Next 2 = 0)
                    then if debug then printfn "XXX Reset state of %s" toCrash.Name
                         //toCrash.AState <- Participant.freshAState
        let () = participants |> Seq.iter Participant.decCrashedFor
        unfinished <- unfinishedParticipants participants
        let (canHandle,canSend,reqsWaiting) = unfinished
        c <- not (Array.isEmpty canHandle) || not (Array.isEmpty canSend) || not (Array.isEmpty reqsWaiting)
        step <- step + 1
  
  let drainQueue (buf:Queue<'a>) = 
    let mutable l = []
    while buf.Count <> 0
      do l <- buf.Dequeue () :: l
    l

