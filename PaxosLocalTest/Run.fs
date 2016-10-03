namespace Paxos.BasicPaxos.Test

open Paxos.BasicPaxos
open Paxos.BasicPaxos.Test
open System.Collections.Generic


//Ideas for testing
// - Reset nodes
// - Node outage
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

  let consumeRequest (p:Participant.Proposer) (MClientRequest cr) =
    

  let consumeMsg debug quorumSize participants result (p:Participant.Participant) =
    match p with
    | Participant.Acceptor a ->
      let (sender, msg) = a.Input.Dequeue ()
      let () = if debug then printfn "%s <- %A state: %A" a.Name msg a.AState
      let r = 
        match msg with
        | PMsg pmsg -> let (s',outMsgO) = acceptorReceiveFromProposer a.AState pmsg sender
                       do outMsgO |> Option.map wrapA |> Option.iter (sendMsg participants a.Name)
                          a.AState <- s'
        | AMsg amsg -> let (s',outMsgO) = acceptorReceiveFromAcceptor a.AState amsg sender
                       do outMsgO |> Option.map wrapA |> Option.iter (sendMsg participants a.Name)
                          a.AState <- s'
        | _ -> failwithf "Acceptor received unexpected message %A" msg
      do if debug then printfn "--> %A" a.AState
         r
    | Participant.Proposer p ->
      let (sender, msg) = p.Input.Dequeue ()
      let () = if debug then printfn "%s <- %A state: %A" p.Name msg p.PState
      let r = 
        match msg with
        | CMsg cmsg -> p.InputRequests.Enqueue cmsg
//                       let (s',outMsgO) = proposerReceiveFromClient p.PState cmsg
//                       do outMsgO |> Option.map wrapP |> Option.iter (sendMsg participants p.Name)
//                          p.PState <- s'
        | AMsg amsg -> let (s',outMsgO) = proposerReceiveFromAcceptor quorumSize p.PState amsg
                       do outMsgO |> Option.map wrapP |> Option.iter (sendMsg participants p.Name)
                          p.PState <- s'
        | _ -> failwithf "Proposer received unexpected message %A" msg
      do if debug then printfn "--> %A" p.PState
         r
    | Participant.Learner l ->
      let (sender, msg) = l.Input.Dequeue ()
      let () = if debug then printfn "%s <- %A state: %A" l.Name msg l.LState
      let r = 
        match msg with
        | AMsg amsg -> let (s', outMsgO) = learnerReceiveFromAcceptor l.LState amsg sender quorumSize
                       do outMsgO |> Option.map wrapL |> Option.iter (sendMsg participants l.Name)
                          l.LState <- s'
        | _ -> failwithf "Learner received unexpected message %A" msg
      do if debug then printfn "--> %A" l.LState
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
    in (canHandle,canSend)

  let runToEnd (debug:bool) (random:System.Random) quorumSize participants result = 
    let pickRandom xs =
      let length = Array.length xs
      let i = random.Next length
      in xs.[i]
    let consume = consumeMsg debug quorumSize participants result
    let send = sendMsg participants
    let mutable unfinished = unfinishedParticipants participants //not counting client inputs
    let mutable c = true
    while c
      do
        let (canHandle,canSend) = unfinished
        match random.Next 6 with
        //consume message -- 1 message consumed can result in 4 messages sent (broadcast -- assuming cluster size 3)
        | 0 | 1 | 2 | 3 -> 
          if (Array.isEmpty canHandle)
          then ()
          else let participant = pickRandom canHandle
               consume participant 
        //send message
        | 4 -> 
          if (Array.isEmpty canSend)
          then ()
          else let participant = pickRandom canSend
               let name = Participant.name participant
               let outBuf = Participant.output participant
               send name (outBuf.Dequeue ())
        | 5 -> 
          let proposersWithWaitingReqs = 
            participants 
            |> Seq.choose Participant.tryProposer 
            |> Seq.filter (fun p -> p.InputRequests.Count <> 0)
            |> Seq.toArray
          if (proposersWithWaitingReqs.Length <> 0)
          then let p = pickRandom proposersWithWaitingReqs
               let r = p.InputRequests.Dequeue ()               
               consumeRequest p r
          else ()

          
        | _ (*2*) -> //maybe do something evil
          if (random.Next 20 <> 0)
          then () // P(evil) = 1/5 * 1/20 = 1/100
          else let all =  Participant.allA participants
               let alive = all |> Seq.filter (fun a -> a.CrashedFor = 0) |> Seq.toArray
               if(Seq.length alive <= quorumSize) 
               then () //we cannot crash more according to assumptions
               else let toCrash = pickRandom random alive
                    let rounds = (random.Next 100) + 10 //at least 10 rounds
                    printfn "XXX Crashing %s for %i rounds" toCrash.Name rounds
                    toCrash.Input.Clear()
                    toCrash.CrashedFor <- rounds
                    if (random.Next 2 = 0)
                    then printfn "XXX Reset state of %s" toCrash.Name
                         toCrash.AState <- Participant.freshAState
        let () = participants |> Seq.iter Participant.decCrashedFor
        unfinished <- unfinishedParticipants participants
        let (canHandle,canSend) = unfinished
        c <- not (Array.isEmpty canHandle) || not (Array.isEmpty canSend)
  
  let drainQueue (buf:Queue<'a>) = 
    let mutable l = []
    while buf.Count <> 0
      do l <- buf.Dequeue () :: l
    l

