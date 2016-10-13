namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open System.Collections.Generic



module Format =

  let pState (s:PState) =
    match s with
    | PReady n -> sprintf "PReady %i" n
    | PPrepareSent (n,cr,vvos) -> sprintf "PPrepareSent %i [%i]" n vvos.Length
    | PAcceptSent (n,cr,vvs) -> sprintf "PAcceptSent %i [%i]" n vvs.Length

  let cState (s:CState) =
    match s with
    | CReady -> "CInitial"
    | CActiveRequest (q,req,t,(session,(fname,f))) -> sprintf "CActiveRequest q=%i, t=%i, f=%s" q.Count t fname

  let aState (s:AState) =
    match s with
      | AReady (n,store) -> sprintf "AReady (%i, %A)" n store
      | ASync (ns, store, votes) -> sprintf "%A" s

  let lState (LReady store) =
    sprintf "%A" store

  let pMsg (m:PMsg) = 
    match m with
    | MPrepare (n,k) -> sprintf "MPrepare (%i,%s)" n k
    | MAccept (n,sender,(g,k),v) -> sprintf "MAccept (%i,%s,%A)" n k v

  let aMsg (m:AMsg) =
    match m with
    | MPromise (n,vvo) -> sprintf "MPromise (%i,%A)" n vvo
    | MAccepted (n,sender,(g,k),v) -> sprintf "MAccepted (%i,%s,%A)" n k v
    | MNackPrepare (n,n') -> sprintf "MNackPrepare (%i,%i)" n n'
    | MSyncRequest -> sprintf "MSyncRequest"
    | MSyncResponse (n,store)-> sprintf "MSyncResponse (%i,%A)" n store

  let lMsg (m:LMsg) =
    match m with
    | MResponse (g,v) -> sprintf "MResponse (%s,%A)" (g.ToString ()) v

  let eMsg (m:EMsg) =
    match m with
    | MExternalRequest (sender, ((g,k),f)) -> sprintf "MExternalRequest (%s,%s)" (g.ToString()) k

  let msg (m:Msg) =
    match m with
    | AMsg x -> aMsg x
    | PMsg x -> pMsg x
    | LMsg x -> lMsg x
    | EMsg x -> eMsg x

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
    | Participant.Proposer x -> sprintf "%s: in=%i, out=%i, %s %s" x.Name x.Input.Count x.Output.Count (cState x.CState) (pState x.PState)
    | Participant.Acceptor x -> sprintf "%s: %sin=%i, out=%i, %s" x.Name (crashed x.CrashedFor) x.Input.Count x.Output.Count (aState x.AState)
    | Participant.Learner x -> sprintf "%s: in=%i, out=%i, %s" x.Name x.Input.Count x.Output.Count (lState x.LState)
    | Participant.External x -> sprintf "%s: in=%i, out=%i" x.Name x.Input.Count x.Output.Count

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
        | _ -> failwith "a msg from a non-learner sent to result"
      result.Responses |> Seq.choose f
  
    let responsesToClient name result =
      result.Responses |> Seq.filter (fun (n,_,_) -> n = name)


  let externalReceive (entry:Participant.External) m =
    entry.Output.Enqueue m

  //puts the msg in the right input queue
  let sendMsg participants sender m = 
    let send msg d = Participant.sendToDest (sender,msg) d participants
    let broadcast msg = Participant.broadcast participants (sender,msg)
    let broadcastA msg = Participant.broadcastA participants (sender,msg)
    match m with
    | (Proposer d, (EMsg (MExternalRequest _) as msg)) -> send msg d
    | (BroadcastAcceptors, (PMsg (MPrepare _) as msg)) -> broadcastA msg
    | (Proposer d, (AMsg (MPromise _) as msg)) -> send msg d
    | (BroadcastAcceptors, (PMsg (MAccept _) as msg)) -> broadcastA msg
    | (Proposer d, (AMsg (MNackPrepare _) as msg)) -> send msg d
    | (Broadcast, (AMsg (MAccepted _) as msg)) -> broadcast msg
    | (External d, (LMsg (MResponse _) as msg)) -> send msg d
    | (BroadcastAcceptors, (AMsg (MSyncRequest _) as msg)) -> broadcastA msg
    | (Acceptor d, (AMsg (MSyncResponse _) as msg)) -> send msg d
    | _ -> failwithf "bad message destination %A" m
  
  let wrapA (d,m) = (d,AMsg m)
  let wrapP (d,m) = (d,PMsg m)
  let wrapL (d,m) = (d,LMsg m)
  let wrapE (d,m) = (d,EMsg m)
  

  let consumeMsg debug quorumSize result time (p:Participant.Participant) =
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
      | AMsg amsg -> let (s',outMsgO) = acceptorReceiveFromAcceptor 2 quorumSize a.AState amsg sender
                     do let outDestMsgO = outMsgO |> Option.map wrapA
                        printDebug s' outDestMsgO
                        Option.iter a.Output.Enqueue outDestMsgO
                        a.AState <- s'
      | _ -> failwithf "Acceptor received unexpected message %A" msg
    | Participant.Proposer p ->
      let (sender, msg) = p.Input.Dequeue ()
      let printDebug c' s' outDestMsgO = 
        if debug 
        then printfn "%s <- (%s,%s) --> (%s,%s) out: %s" p.Name sender (Format.msg msg) (Format.cState c') (Format.pState s') (Format.option Format.destMsg outDestMsgO)
      match msg with
      | EMsg emsg -> let (cs',ps',outMsgO) = proposerReceiveRequest p.CState p.PState time emsg
                     do let outDestMsgO = outMsgO |> Option.map wrapP
                        printDebug cs' ps' outDestMsgO
                        Option.iter p.Output.Enqueue outDestMsgO
                        p.PState <- ps'
                        p.CState <- cs'
      | AMsg amsg -> let (cs',ps',outMsgO) = proposerReceiveFromAcceptor quorumSize time p.CState p.PState amsg
                     do let outDestMsgO = outMsgO |> Option.map wrapP
                        printDebug p.CState ps' outDestMsgO
                        Option.iter p.Output.Enqueue outDestMsgO
                        p.PState <- ps'
                        p.CState <- cs'
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
    | Participant.External c ->
      let (sender, msg) = c.Input.Dequeue ()
      result.Responses <- (c.Name, sender, msg) :: result.Responses

  let checkTimeout debug (p:Participant.Proposer) t hasTimedOut =
    let (cs',ps',outMsgO) = clientCheckActiveRequest p.CState p.PState t hasTimedOut
    let outDestMsgO = outMsgO |> Option.map wrapP
    //if debug then printfn "Check timeout, %s --> (%s,%s) out: %s" p.Name (Format.cState cs') (Format.pState ps') (Format.option Format.destMsg outDestMsgO)
    p.CState <- cs'
    p.PState <- ps'
    Option.iter p.Output.Enqueue (outDestMsgO)

  let decCrashedForAndWakeup debug p = 
    match p with 
    | Participant.Acceptor x -> 
      if x.CrashedFor = 1 
      then let (s',m) = acceptorRestart ()
           x.Output.Enqueue (wrapA m)
           x.AState <- s'
           x.CrashedFor <- 0
           //if debug then printfn "Waking up %s..." x.Name

      if x.CrashedFor > 1 then x.CrashedFor <- x.CrashedFor - 1
    | Participant.Proposer x -> ()
    | Participant.Learner x -> ()
    | Participant.External x -> () 

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

    in (canHandle,canSend)

  let hasTimedOut now reqTime = 
    now - reqTime > 200
    
  let runToEnd (debugIn:bool) (random:System.Random) emulateMessageLosses quorumSize participants result = 
    let mutable debug = debugIn
    let pickRandom xs =
      let length = Array.length xs
      let i = random.Next length
      in xs.[i]
    let calcNotDone () = participants |> Seq.forall Participant.isDone |> not 
    let send = sendMsg participants
    
    let mutable unfinished = unfinishedParticipants participants //not counting client inputs
    let mutable notDone = participants |> Seq.forall Participant.isDone |> not
    let mutable time = 0
    while notDone
      do
        //printfn "-------------------------\n%A" participants
//        if time = 0 then debug <- true
//        if time = 15000 then debug <- false
        participants |> Seq.iter (decCrashedForAndWakeup debug)

        let (canHandle,canSend) = unfinished
        if (Array.isEmpty canHandle) then printf ""

        match random.Next 5 with
        |0|1|2|3 -> //progress
          match random.Next 6 with
          //consume message -- 1 message consumed can result in 4 messages sent (broadcast -- assuming cluster size 3)
          | 0 | 1 | 2 -> 
            if (Array.isEmpty canHandle)
            then ()//if debug then printfn "nop - nothing to handle" else ()
            else let participant = pickRandom canHandle
                 if debug then printfn "-------------- %i --------------\n%s" time (Format.participants participants)
                 consumeMsg debug quorumSize result time participant
          //send message
          | 3 | 4 -> 
            if (Array.isEmpty canSend)
            then ()//if debug then printfn "nop - nothing to send" else ()
            else let participant = pickRandom canSend
                 let name = Participant.name participant
                 let outBuf = Participant.output participant
                 let (d,m) = (outBuf.Dequeue ())
                 //if debug then printfn "Send msg: %s, %s" (Participant.name participant) (Format.destMsg (d,m))
                 send name (d,m)
          //check timeouts
          | _ -> 
            let p = pickRandom (participants |> Seq.choose Participant.tryProposer |> Seq.toArray)
            in checkTimeout debug p time (hasTimedOut time)
        | _ -> //maybe do something evil
          if (random.Next 50 <> 0)
          then () // P(evil) = 1/5 * 1/20 = 1/100
          else let all =  Participant.allA participants
               let alive = all |> Seq.filter Participant.isAlive |> Seq.toArray
               if(Seq.length alive <= quorumSize) 
               then ()//if debug then printfn "nop - too many crashed" //we cannot crash more according to assumptions
               else let toCrash = pickRandom alive
                    let rounds = (random.Next 100) + 10 //at least 10 rounds
                    //if debug then printfn "XXX Crashing %s for %i rounds" toCrash.Name rounds
                    if emulateMessageLosses then toCrash.Input.Clear()
                    toCrash.CrashedFor <- rounds
                    if (random.Next 2 = 0)
                    then ()//if debug then printfn "XXX Reset state of %s" toCrash.Name
                         //toCrash.AState <- Participant.freshAState
        let () = participants |> Seq.iter (decCrashedForAndWakeup debug)
        unfinished <- unfinishedParticipants participants
        notDone <- calcNotDone()
        time <- time + 1
//    printfn "time:%i" time
  
  let drainQueue (buf:Queue<'a>) = 
    let mutable l = []
    while buf.Count <> 0
      do l <- buf.Dequeue () :: l
    l

